# ======================================================================
# NEWSFLOWS – Engagement Harvester (Bluesky/ATProto, Public Endpoints)
# ----------------------------------------------------------------------
# Purpose
#   Given a user DID and a time window start ("since"), collect that user's
#   public engagements (likes, reposts, quotes, comments) *with specific bot
#   accounts* and return a tidy tibble.
#
# Core idea (per HTTP reference):
#   • Public reading of a user's repo is allowed via
#       com.atproto.repo.listRecords (must be called on the user's PDS)
#   • Post schemas indicate quotes/replies via `embed` and `reply` fields
#   • AppView endpoints aid with identity resolution, but do not serve
#     listRecords for arbitrary repos
#
# What we DO use (no auth):
#   • com.atproto.identity.resolveDid       (AppView)  – DID → DID Document
#   • com.atproto.identity.resolveIdentity  (AppView)  – identity/handle → DID
#   • com.atproto.identity.resolveHandle    (AppView)  – handle → DID (bots)
#   • com.atproto.repo.listRecords          (User PDS) – likes/posts/reposts
#   • app.bsky.actor.getProfile             (AppView)  – sanity check existence
#
# What we deliberately do NOT use:
#   • app.bsky.feed.getActorLikes – requires auth as the actor; not suitable
#     for third-party reads. We read likes from the user's repo instead.
#
# Flow
#   1) Normalise `since` (POSIXt/Date/difftime/ISO; numeric = days ago)
#   2) Resolve the user's PDS host from DID (AppView → PLC → AppView fallback)
#   3) Resolve bot handles → DIDs (AppView)
#   4) Pull collections from user repo on their PDS:
#        - app.bsky.feed.like    → like subjects
#        - app.bsky.feed.repost  → repost subjects
#        - app.bsky.feed.post    → infer quotes/replies from record fields
#   5) Unify, then filter by time and target author DID ∈ bot DIDs
#
# Notes & caveats
#   • Embeds are an open union. Quotes can be `embed.record` or
#     `embed.recordWithMedia`. We branch on `$type` and read the strongRef uri
#     with a fallback to `$link` for unusual fixtures.
#   • DID methods may vary (did:plc, did:web, …); we use a relaxed regex when
#     extracting the author DID from at:// URIs.
#   • listRecords MUST be called on the actor's PDS host, not AppView.
#   • Federation/rate limits exist. We use `req_retry` with exponential backoff.
#
# Output columns
#   timestamp (POSIXct, UTC)
#   did_engagement (chr) – the querying user DID
#   post_uri (chr)       – at:// URI of the engaged post
#   post_author_handle (chr) – the matched bot handle (for readability)
#   engagement_type (chr)    – one of: like | repost | quote | comment
#   engagement_text (chr)    – text for quotes/comments (NA for likes/reposts)
#
# Example
#   get_user_engagements_with_bots("did:plc:…", since = 24, verbose = TRUE)
# ======================================================================

# --- 1. SETUP ----------------------------------------------------------------
# Packages required (loaded lazily via `pkg::fun` to avoid altering global
# search paths): httr2, purrr, memoise, tibble, dplyr, lubridate, stringr, cli.

# --- 2. CONFIGURATION ---
BOT_HANDLES <- c(
  "news-flows-nl.bsky.social",
  "news-flows-ir.bsky.social",
  "news-flows-cz.bsky.social",
  "news-flows-fr.bsky.social"
)

# --- 3. REUSABLE API HELPERS ---

#' Perform a GET against the AppView host with retries
#'
#' @param path Character, path beginning with `/xrpc/…`.
#' @param query Named list of query parameters.
#' @return Parsed JSON (list). Errors are thrown (caught upstream).
#' @details AppView is used for identity/profile lookups; it does not proxy
#'   `com.atproto.repo.listRecords` for arbitrary repos.
appview_request <- function(path, query = list()) {
  tryCatch({
    httr2::request("https://public.api.bsky.app") |>
      httr2::req_url_path(path) |>
      httr2::req_url_query(!!!purrr::compact(query)) |>
      httr2::req_headers("User-Agent" = "newsflows-engagements/1.0") |>
      httr2::req_retry(max_tries = 5, backoff = ~ min(60, 1.6^.x)) |>
      httr2::req_perform() |>
      httr2::resp_body_json(check_type = FALSE)
  }, error = function(e) {
    # Instead of just returning NULL, re-throw the error so tryCatch can see it
    stop(e)
  })
}

#' Collect an entire repo collection via pagination (public, no auth)
#'
#' @param repo_did DID of the actor whose repo to read.
#' @param collection_name One of `app.bsky.feed.like`, `app.bsky.feed.post`,
#'   or `app.bsky.feed.repost`.
#' @return A list of record objects as returned by `listRecords`.
#' @details `com.atproto.repo.listRecords` must be called on the actor's PDS
#'   host (not AppView). This helper resolves the PDS and walks `cursor` until
#'   exhaustion, returning the concatenated `records`.
collect_paginated_repo <- function(repo_did, collection_name) {
  host <- resolve_pds_endpoint(repo_did)
  if (is.na(host) || !nzchar(host)) {
    stop(sprintf("Could not resolve PDS endpoint for %s", repo_did))
  }

  cursor <- NULL
  records <- list()

  repeat {
    res <- pds_request(
      host,
      path = "/xrpc/com.atproto.repo.listRecords",
      query = list(repo = repo_did, collection = collection_name, limit = 100, cursor = cursor)
    )
    chunk <- res$records
    if (is.null(chunk) || !length(chunk)) {
      break
    }
    records <- c(records, chunk)
    cursor <- res$cursor
    if (is.null(cursor)) {
      break
    }
  }

  return(records)
}

#' Resolve a handle to a DID (memoised)
#'
#' @param handle Bluesky handle (e.g., `news-flows-nl.bsky.social`).
#' @return Character DID or NULL on failure.
resolve_handle_memoised <- memoise::memoise(function(handle) {
  tryCatch({
    res <- appview_request(
      "/xrpc/com.atproto.identity.resolveHandle",
      list(handle = handle)
    )
    purrr::pluck(res, "did")
  }, error = function(e) { NULL })
})

#' Resolve a DID to its PDS base URL (memoised)
#'
#' @param did Actor DID.
#' @return Character base URL (e.g., `https://bsky.social`) or `NA_character_`.
#' @details Tries AppView `resolveDid` first, then PLC DID doc, then AppView
#'   `resolveIdentity`. Accepts service entries with `type` of
#'   `#atproto_pds` or `AtprotoPersonalDataServer`, or `id` ending in
#'   `#atproto_pds`. Prefers `serviceEndpoint`, falls back to `endpoint`.
resolve_pds_endpoint <- memoise::memoise(function(did) {
  extract_endpoint <- function(services) {
    if (is.null(services) || !length(services)) return(NA_character_)
    # Accept historical and current shapes: filter by type or id suffix
    idx <- purrr::detect_index(services, function(svc) {
      t <- toString(purrr::pluck(svc, "type", .default = ""))
      i <- toString(purrr::pluck(svc, "id",   .default = ""))
      any(t %in% c("#atproto_pds", "AtprotoPersonalDataServer")) || grepl("#atproto_pds$", i, ignore.case = TRUE)
    })
    entry <- if (idx > 0) services[[idx]] else NULL
    if (is.null(entry)) return(NA_character_)
    endpoint <- purrr::pluck(entry, "serviceEndpoint", .default = purrr::pluck(entry, "endpoint", .default = NA_character_))
    if (!is.na(endpoint)) endpoint <- sub("/$", "", endpoint)
    endpoint
  }

  # Try AppView resolveDid first (fast, works for did:plc & did:web)
  did_doc_appview <- tryCatch(
    appview_request("/xrpc/com.atproto.identity.resolveDid", list(did = did)),
    error = function(e) NULL
  )
  if (!is.null(did_doc_appview)) {
    ep <- extract_endpoint(purrr::pluck(did_doc_appview, "didDocument", "service", .default =
                                       purrr::pluck(did_doc_appview, "didDoc", "service", .default = list())))
    if (!is.na(ep) && nzchar(ep)) return(ep)
  }

  plc_endpoint <- tryCatch({
    doc <- httr2::request(paste0("https://plc.directory/", did)) |>
      httr2::req_headers(Accept = "application/json") |>
      httr2::req_perform() |>
      httr2::resp_body_json(check_type = FALSE)
    extract_endpoint(purrr::pluck(doc, "service", .default = list()))
  }, error = function(e) {
    NA_character_
  })

  if (!is.na(plc_endpoint) && nzchar(plc_endpoint)) {
    return(plc_endpoint)
  }

  identity_doc <- tryCatch(
    appview_request("/xrpc/com.atproto.identity.resolveIdentity", list(identity = did)),
    error = function(e) NULL
  )

  if (is.null(identity_doc)) {
    return(NA_character_)
  }

  services <- purrr::pluck(identity_doc, "didDocument", "service", .default =
                           purrr::pluck(identity_doc, "didDoc", "service", .default = list()))
  extract_endpoint(services)
})

#' Perform a GET against a user's PDS host with retries
#'
#' @param host Base URL of the PDS (no trailing slash).
#' @param path Path beginning with `/xrpc/…`.
#' @param query Named list of query parameters.
#' @return Parsed JSON (list). Errors bubble up.
pds_request <- function(host, path, query = list()) {
  httr2::request(paste0(host, path)) |>
    httr2::req_url_query(!!!purrr::compact(query)) |>
    httr2::req_headers("User-Agent" = "newsflows-engagements/1.0") |>
    httr2::req_retry(max_tries = 5, backoff = ~ min(60, 1.6^.x)) |>
    httr2::req_perform() |>
    httr2::resp_body_json(check_type = FALSE)
}

#' Check if a user's profile exists on AppView
#'
#' @param did Actor DID or handle.
#' @return Logical, TRUE if the profile can be fetched.
user_exists <- function(did) {
  tryCatch({
    res <- appview_request("/xrpc/app.bsky.actor.getProfile", query = list(actor = did))
    return(!is.null(res))
  }, error = function(e) { FALSE })
}


# --- 4. DATA PROCESSING HELPERS ---

#' Tidy likes from raw repo records
#'
#' @param records List of `app.bsky.feed.like` records from `listRecords`.
#' @return Tibble with columns: engagement_type, timestamp, post_uri.
#' @details Parses `value.createdAt` and `value.subject.uri`. Missing fields are
#'   handled defensively and yield `NA` values.
process_likes_from_repo <- function(records) {
  if (!length(records)) {
    return(tibble::tibble(
      engagement_type = character(),
      timestamp = lubridate::as_datetime(numeric(), tz = "UTC"),
      post_uri = character()
    ))
  }
  purrr::map_dfr(records, ~ tibble::tibble(
    engagement_type = "like",
    timestamp = lubridate::ymd_hms(purrr::pluck(.x, "value", "createdAt", .default = NA_character_), tz = "UTC", quiet = TRUE),
    post_uri = purrr::pluck(.x, "value", "subject", "uri", .default = NA_character_)
  ))
}

#' Coerce a possibly-missing value to scalar character or NULL
#'
#' @param x Any value possibly NULL/list/character.
#' @return Scalar character or NULL.
safe_char <- function(x) {
  if (is.null(x) || length(x) == 0) return(NULL)
  if (is.character(x) && !is.na(x[1])) return(x[1])
  NULL
}

#' Extract the quoted post URI from a post `embed`
#'
#' @param embed The `value.embed` object from a post record.
#' @return Character at:// URI or NULL if not a quote.
#' @details Handles both `app.bsky.embed.record` and
#'   `app.bsky.embed.recordWithMedia`, preferring strongRef `uri` with a
#'   `$link` fallback for odd/legacy fixtures.
extract_quoted_uri <- function(embed) {
  if (is.null(embed)) return(NULL)
  et <- purrr::pluck(embed, "$type", .default = NULL)
  if (is.null(et)) return(NULL)
  if (et == "app.bsky.embed.record") {
    # Prefer strongRef uri; fall back to $link for older/edge fixtures
    return(
      safe_char(
        purrr::pluck(embed, "record", "uri", .default =
          purrr::pluck(embed, "record", "$link", .default = NULL)
        )
      )
    )
  }
  if (et == "app.bsky.embed.recordWithMedia") {
    # recordWithMedia nests the strongRef under record.record.uri; also fallback to $link
    return(
      safe_char(
        purrr::pluck(embed, "record", "record", "uri", .default =
          purrr::pluck(embed, "record", "record", "$link", .default = NULL)
        )
      )
    )
  }
  NULL
}

#' Tidy quotes and comments from raw post records
#'
#' @param records List of `app.bsky.feed.post` records from `listRecords`.
#' @return Tibble with columns: engagement_type (quote/comment), timestamp,
#'   engagement_text, post_uri.
#' @details Determines `quote` via `embed` union; determines `comment` via
#'   `value.reply.parent.uri`. Text is kept for auditing.
process_posts <- function(records) {
  if (!length(records)) {
    return(tibble::tibble(
      engagement_type = character(),
      timestamp       = lubridate::as_datetime(numeric(), tz = "UTC"),
      engagement_text = character(),
      post_uri        = character()
    ))
  }

  purrr::map_dfr(records, function(rec) {
    val <- rec$value

    # Robust extraction of quoted URI across embed variants
    quoted_uri <- extract_quoted_uri(val$embed)

    # Replies/comments point to a parent URI
    reply_uri  <- purrr::pluck(val, "reply", "parent", "uri", .default = NULL)

    type <- dplyr::case_when(
      !is.null(quoted_uri) ~ "quote",
      !is.null(reply_uri)  ~ "comment",
      TRUE ~ NA_character_
    )

    tibble::tibble(
      engagement_type = type,
      timestamp       = lubridate::ymd_hms(purrr::pluck(val, "createdAt", .default = NA_character_), tz = "UTC", quiet = TRUE),
      engagement_text = purrr::pluck(val, "text", .default = NA_character_),
      post_uri        = {
        if (!is.na(type) && type == "quote") {
          if (is.null(quoted_uri)) NA_character_ else as.character(quoted_uri)
        } else if (!is.na(type) && type == "comment") {
          if (is.null(reply_uri)) NA_character_ else as.character(reply_uri)
        } else {
          NA_character_
        }
      }
    )
  }) |>
    dplyr::mutate(post_uri = as.character(post_uri)) |>
    dplyr::filter(!is.na(engagement_type))
}

#' Tidy reposts from raw repo records
#'
#' @param records List of `app.bsky.feed.repost` records from `listRecords`.
#' @return Tibble with columns: engagement_type, timestamp, post_uri.
process_reposts <- function(records) {
  if (!length(records)) {
    return(tibble::tibble(
      engagement_type = character(),
      timestamp = lubridate::as_datetime(numeric(), tz = "UTC"),
      post_uri = character()
    ))
  }
  purrr::map_dfr(records, ~ tibble::tibble(
    engagement_type = "repost",
    timestamp = lubridate::ymd_hms(purrr::pluck(.x, "value", "createdAt", .default = NA_character_), tz = "UTC", quiet = TRUE),
    post_uri = purrr::pluck(.x, "value", "subject", "uri", .default = NA_character_)
  ))
}

# --- 5. ROBUST FETCHING WITH FALLBACK ---

#' Fetch and tidy a user's likes (repo-based)
#'
#' @param did Actor DID.
#' @return Tibble of likes (may be empty on failure).
#' @details Wraps `collect_paginated_repo()` with error handling; logs a warning
#'   and returns an empty tibble on failure.
fetch_user_likes <- function(did) {
  tryCatch({
    records <- collect_paginated_repo(repo_did = did, collection_name = "app.bsky.feed.like")
    process_likes_from_repo(records)
  }, error = function(e) {
    warning(sprintf("Failed to fetch like records for %s: %s", did, conditionMessage(e)))
    tibble::tibble()
  })
}


# --- 6. MAIN FUNCTION ---

#' Collect a user's engagements with specified bot accounts
#'
#' @param did Actor DID of the engaging user.
#' @param since Start of time window. Accepts POSIXt/Date/difftime/ISO string;
#'   numeric is interpreted as days ago (e.g., 1   = last 24 hours,
#'   0.5 = last 12 hours).
#' @param verbose Logical; if TRUE, prints progress and diagnostics.
#' @return Tibble with columns:
#'   `timestamp`, `did_engagement`, `post_uri`, `post_author_handle`,
#'   `engagement_type` (like/repost/quote/comment), `engagement_text`.
#' @examples
#' \dontrun{
#'   did <- "did:plc:3vomhawgkjhtvw4euuxbll3r"
#'   get_user_engagements_with_bots(did, since = 1, verbose = TRUE)
#' }
#' @seealso `com.atproto.repo.listRecords` (PDS), `app.bsky.actor.getProfile` (AppView)
#' @details Internally resolves the user's PDS; fetches likes/posts/reposts from
#'   their repo; infers quotes/comments from post schema; filters to target bot
#'   DIDs and time window; and joins back the bot handle for readability.
get_user_engagements_with_bots <- function(did, since, verbose = TRUE) {

  normalize_since <- function(value) {
    # Justification:
    # - Numeric inputs previously interpreted as seconds-since-epoch, so `since=1` became 1970-01-01.
    # - We now interpret numeric as "days ago" (e.g., 1 = last 24 hours) and accept difftime objects.
    if (inherits(value, "POSIXt")) {
      return(lubridate::with_tz(value, tzone = "UTC"))
    }
    if (inherits(value, "Date")) {
      return(lubridate::as_datetime(value, tz = "UTC"))
    }
    if (inherits(value, "difftime")) {
      return(lubridate::now(tzone = "UTC") - value)
    }
    if (is.numeric(value) && length(value) == 1L && !is.na(value)) {
      if (value < 0) {
        stop("`since` numeric value must be non-negative (interpreted as days ago).")
      }
      # Treat numeric as days ago for ergonomic CLI use (e.g., since = 1 → last 24 hours)
      return(lubridate::now(tzone = "UTC") - lubridate::ddays(value))
    }
    if (is.character(value) && length(value) == 1L && nzchar(value)) {
      parsed <- suppressWarnings(lubridate::ymd_hms(value, tz = "UTC", quiet = TRUE))
      if (!is.na(parsed)) {
        return(parsed)
      }
      parsed <- suppressWarnings(lubridate::as_datetime(value, tz = "UTC"))
      if (!is.na(parsed)) {
        return(parsed)
      }
    }
    stop("`since` must be a POSIXt/Date, difftime, ISO datetime string, or number of days ago.")
  }

  since_utc <- normalize_since(since)

  empty_result <- tibble::tibble(
    timestamp = lubridate::as_datetime(numeric(), tz = "UTC"), did_engagement = character(),
    post_uri = character(), post_author_handle = character(),
    engagement_type = character(), engagement_text = character()
  )

  if (verbose) {
    cli::cli_h1("Collecting Engagements for {.val {did}}")
    cli::cli_alert_info("Window start: {.val {format(since_utc, '%Y-%m-%d %H:%M:%SZ')}}")
  }

  # Resolve and display the PDS of the requesting user (helps with troubleshooting)
  pds_host <- resolve_pds_endpoint(did)
  if (verbose) {
    if (!is.na(pds_host) && nzchar(pds_host)) {
      cli::cli_alert_info("Resolved PDS host: {.url {pds_host}}")
    } else {
      cli::cli_alert_warning("Could not resolve PDS host for {.val {did}}; subsequent repo calls will fail.")
    }
  }
  if (is.na(pds_host) || !nzchar(pds_host)) {
    return(empty_result)
  }

  if (verbose) {
    cli::cli_progress_step("Verifying user exists on public API")
  }
  if (!user_exists(did)) {
    if (verbose) {
      cli::cli_alert_danger("User with DID {.val {did}} could not be found.")
      cli::cli_progress_done()
    }
    return(empty_result)
  }

  if (verbose) {
    cli::cli_progress_step("Resolving bot handles")
  }
  bot_handle_map <- purrr::set_names(purrr::map_chr(BOT_HANDLES, resolve_handle_memoised), BOT_HANDLES)
  bot_dids <- as.character(na.omit(bot_handle_map))

  if (!length(bot_dids)) {
    if (verbose) {
      cli::cli_alert_warning("None of the bot handles could be resolved; returning empty result.")
      cli::cli_progress_done()
    }
    return(empty_result)
  }

  if (verbose) {
    cli::cli_progress_step("Fetching user activity from PDS")
  }

  likes_records <- fetch_user_likes(did)
  post_records <- tryCatch(collect_paginated_repo(did, "app.bsky.feed.post"), error = function(e) list())
  repost_records <- tryCatch(collect_paginated_repo(did, "app.bsky.feed.repost"), error = function(e) list())

  if (verbose) {
    cli::cli_progress_step("Processing and filtering user activity")
  }

  engagements <- dplyr::bind_rows(
    likes_records,
    process_posts(post_records),
    process_reposts(repost_records)
  )
  
  # Filter early and use a relaxed DID regex to be future-proof (did:web, etc.)
  engagements <- engagements |>
    dplyr::filter(!is.na(timestamp), !is.na(post_uri)) |>
    dplyr::mutate(
      post_author_did = stringr::str_extract(post_uri, "(?<=at://)(did:[a-z0-9:._-]+)")
    ) |>
    dplyr::filter(post_author_did %in% bot_dids, timestamp >= since_utc)

  if (nrow(engagements) == 0) {
    if (verbose) {
      cli::cli_alert_success("User has no relevant activity in the time window.")
      cli::cli_progress_done()
    }
    return(empty_result)
  }

  final_results <- engagements |>
    dplyr::mutate(did_engagement = did) |>
    dplyr::left_join(
      tibble::enframe(bot_handle_map, name = "post_author_handle", value = "post_author_did"),
      by = "post_author_did"
    ) |>
    dplyr::select(
      timestamp, did_engagement, post_uri, post_author_handle,
      engagement_type, engagement_text
    ) |>
    dplyr::arrange(timestamp)
  
  if (verbose) {
    cli::cli_progress_done()
    cli::cli_alert_success("Found {nrow(final_results)} engagements with target bots.")
  }
  
  return(final_results)
}
