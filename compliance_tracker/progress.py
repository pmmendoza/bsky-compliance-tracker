"""Progress iterator helpers with safe length detection."""

from __future__ import annotations

from typing import Iterable, Iterator, Optional, Protocol, runtime_checkable

try:
    from tqdm import tqdm  # type: ignore
except ImportError:  # pragma: no cover - tqdm optional
    tqdm = None  # type: ignore


@runtime_checkable
class _SizedIterable(Protocol):
    def __len__(self) -> int: ...


def progress_iter(iterable: Iterable, *, total: Optional[int] = None, desc: str = "") -> Iterable:
    """Wrap the iterable with a progress indicator if available."""

    if total is None and isinstance(iterable, _SizedIterable):
        try:
            total = len(iterable)
        except TypeError:
            total = None

    if tqdm is not None:
        return tqdm(iterable, total=total, desc=desc, leave=False)

    return _simple_progress_iter(iterable, total=total, desc=desc)


def _simple_progress_iter(iterable: Iterable, *, total: Optional[int], desc: str) -> Iterator:
    count = 0
    bar_width = 30
    prefix = f"{desc}: " if desc else ""
    iterator = iter(iterable)
    for item in iterator:
        yield item
        count += 1
        _print_progress(prefix, count, total, bar_width)
    _print_completion(prefix, count, total, bar_width)


def _print_progress(prefix: str, count: int, total: Optional[int], bar_width: int) -> None:
    import sys

    if total and total > 0:
        filled = min(bar_width, int(bar_width * count / total))
        bar = "#" * filled + "-" * (bar_width - filled)
        sys.stderr.write(f"\r{prefix}[{bar}] {count}/{total}")
    else:
        sys.stderr.write(f"\r{prefix}{count} items")
    sys.stderr.flush()


def _print_completion(prefix: str, count: int, total: Optional[int], bar_width: int) -> None:
    import sys

    if total and total > 0:
        bar = "#" * bar_width if count else "-" * bar_width
        sys.stderr.write(f"\r{prefix}[{bar}] {count}/{total}\n")
    else:
        sys.stderr.write(f"\r{prefix}{count} items\n")
    sys.stderr.flush()
