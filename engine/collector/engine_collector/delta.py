"""Turn cumulative counter snapshots into per-poll deltas."""

from __future__ import annotations


def row_key(row: dict, key: tuple[str, ...]) -> tuple:
    return tuple(row.get(k) for k in key)


def diff(previous: dict[tuple, dict], current: list[dict], key: tuple[str, ...],
         counters: tuple[str, ...]) -> tuple[list[dict], dict[tuple, dict]]:
    """Return (delta_rows, new_previous).

    For each current row, counters become current - previous for the same
    key. A key seen for the first time, or one whose counters went down (the
    source was reset or the entry was evicted and re-created), reports its
    current values as the delta. Rows whose counters all have zero delta are
    dropped. Non-counter columns pass through unchanged.

    new_previous holds the current rows by key and replaces previous; keys
    that disappeared are forgotten.
    """
    deltas: list[dict] = []
    seen: dict[tuple, dict] = {}
    for row in current:
        k = row_key(row, key)
        seen[k] = row
        before = previous.get(k)
        out = dict(row)
        if before is not None and not _went_down(before, row, counters):
            for c in counters:
                out[c] = (row.get(c) or 0) - (before.get(c) or 0)
        else:
            for c in counters:
                out[c] = row.get(c) or 0
        if any(out[c] for c in counters):
            deltas.append(out)
    return deltas, seen


def _went_down(before: dict, after: dict, counters: tuple[str, ...]) -> bool:
    return any((after.get(c) or 0) < (before.get(c) or 0) for c in counters)
