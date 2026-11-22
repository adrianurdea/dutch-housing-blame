"""Collect all posts and comments of two subreddits from the Arctic Shift API.

The API's keyword search times out on these large subreddits, so we download
everything in weekly windows and filter for housing later (in the notebook).

Design:
- One output file per (kind, subreddit, week): data/raw/<kind>/<subreddit>/<week_start>.jsonl.zst
- Resumable: a week whose file already exists is skipped. Files are written to
  a .part file first and renamed only when the week is complete.
- Polite: at most 1 request per second. We ask for limit=auto (the server
  returns 100-1000 items depending on its load), so fewer requests are needed.
- On 429 (rate limit) we wait for the X-RateLimit-Reset header. On 422
  ("Timeout", the query was too heavy right now) and 5xx we retry after a short
  increasing backoff (5, 10, 20, ... s); waiting for the full reset is not needed.
- Weeks that still fail are logged to data/raw/failures.csv and retried on the next run.

Usage (from the project root):
    python collect.py --start 2024-01-01 --end 2026-11-01
    python collect.py --start 2025-06-02 --end 2025-06-09     # one test week
    python collect.py --weeks 2024-01-01 2024-04-01           # specific weeks
"""

import argparse
import csv
import datetime as dt
import json
import time
from pathlib import Path

import requests
import zstandard

API = "https://arctic-shift.photon-reddit.com/api"
HEADERS = {"User-Agent": "VU-SocialWeb-Group20-research/1.0 (university course project)"}
SUBREDDITS = ["thenetherlands", "Netherlands"]
KINDS = ["posts", "comments"]

# Only the fields we need (raw files still contain usernames; they are hashed
# during processing and the raw files never leave the group's SURFdrive).
COMMON_FIELDS = ["id", "author", "author_flair_text", "created_utc", "subreddit", "score",
                 "distinguished", "retrieved_on"]
FIELDS = {
    "posts": COMMON_FIELDS + ["title", "selftext", "num_comments", "link_flair_text", "url", "over_18"],
    "comments": COMMON_FIELDS + ["body", "link_id", "parent_id"],
}

RAW_DIR = Path("data/raw")
FAILURE_LOG = RAW_DIR / "failures.csv"
MIN_INTERVAL = 1.0   # seconds between requests
MAX_RETRIES = 6
MIN_PAGE = 100       # limit=auto returns at least 100 items unless the window is exhausted

_last_request = 0.0


def get_page(kind, subreddit, after, before):
    """Fetch one page (100-1000 items, oldest first) with rate limiting and retries."""
    global _last_request
    params = {"subreddit": subreddit, "after": after, "before": before, "sort": "asc",
              "limit": "auto", "fields": ",".join(FIELDS[kind])}
    for attempt in range(1, MAX_RETRIES + 1):
        # Wait so we never send more than one request per second
        wait = MIN_INTERVAL - (time.time() - _last_request)
        if wait > 0:
            time.sleep(wait)
        _last_request = time.time()
        try:
            r = requests.get(f"{API}/{kind}/search", params=params, headers=HEADERS, timeout=120)
        except requests.RequestException as e:
            error, reset = repr(e), None
        else:
            if r.status_code == 200:
                data = r.json().get("data")
                if data is not None:
                    return data
                error = f"200 without data: {r.text[:200]}"
            else:
                error = f"HTTP {r.status_code}: {r.text[:200]}"
            # Only a real rate limit (429) needs the full reset wait
            reset = r.headers.get("X-RateLimit-Reset") if r.status_code == 429 else None
        # Back off: server's reset time on 429, otherwise 5, 10, 20, ... seconds
        delay = float(reset) + 1 if reset else 5 * 2 ** (attempt - 1)
        print(f"    retry {attempt}/{MAX_RETRIES} in {delay:.0f}s ({error})")
        time.sleep(delay)
    raise RuntimeError(error)


def crawl_window(kind, subreddit, start, end):
    """Return all items created in [start, end), paging forward by created_utc."""
    items, seen = [], set()
    cursor = start - 1  # 'after' is treated as exclusive; we de-duplicate by id anyway
    while True:
        page = get_page(kind, subreddit, cursor, end)
        new = [x for x in page if x["id"] not in seen]
        for x in new:
            seen.add(x["id"])
        items.extend(new)
        if len(page) < MIN_PAGE:
            return items  # last page
        last_ts = page[-1]["created_utc"]
        # Step back 1 second so items sharing the last timestamp are not lost;
        # if a page brought nothing new (>100 items in one second), move on.
        cursor = last_ts - 1 if new and last_ts - 1 > cursor else last_ts


def save_jsonl_zst(items, path):
    """Write items as zstd-compressed JSON lines, atomically (via a .part file)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    with open(tmp, "wb") as f, zstandard.ZstdCompressor(level=10).stream_writer(f) as w:
        for x in items:
            w.write((json.dumps(x, ensure_ascii=False) + "\n").encode("utf-8"))
    tmp.rename(path)


def log_failure(kind, subreddit, week, error):
    new_file = not FAILURE_LOG.exists()
    with open(FAILURE_LOG, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["logged_at", "kind", "subreddit", "week_start", "error"])
        w.writerow([dt.datetime.now(dt.UTC).isoformat(timespec="seconds"), kind, subreddit, week, error])


def weeks(start, end):
    """Yield (week_start, week_end) dates in steps of 7 days; the last week is cut at `end`."""
    d = start
    while d < end:
        yield d, min(d + dt.timedelta(days=7), end)
        d += dt.timedelta(days=7)


def to_epoch(d):
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.UTC).timestamp())


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default="2024-01-01", help="first day (UTC, inclusive)")
    p.add_argument("--end", default="2026-11-01", help="end day (UTC, exclusive)")
    p.add_argument("--subreddits", nargs="+", default=SUBREDDITS)
    p.add_argument("--kinds", nargs="+", default=KINDS, choices=KINDS)
    p.add_argument("--weeks", nargs="+", help="only these week start days (overrides --start/--end)")
    args = p.parse_args()
    start, end = dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end)
    if args.weeks:
        days = [dt.date.fromisoformat(w) for w in args.weeks]
        windows = [(d, min(d + dt.timedelta(days=7), end)) for d in days]
    else:
        windows = list(weeks(start, end))

    for week_start, week_end in windows:
        for subreddit in args.subreddits:
            for kind in args.kinds:
                path = RAW_DIR / kind / subreddit / f"{week_start}.jsonl.zst"
                if path.exists():
                    continue
                t0 = time.time()
                try:
                    items = crawl_window(kind, subreddit, to_epoch(week_start), to_epoch(week_end))
                except RuntimeError as e:
                    print(f"FAILED {kind} r/{subreddit} {week_start}: {e}")
                    log_failure(kind, subreddit, week_start, str(e))
                    continue
                save_jsonl_zst(items, path)
                text_field = "body" if kind == "comments" else "selftext"
                deleted = sum(x.get(text_field) in ("[deleted]", "[removed]") for x in items)
                print(f"{week_start} r/{subreddit:<15} {kind:<8} n={len(items):>6} "
                      f"deleted={deleted / max(len(items), 1):5.1%} "
                      f"{time.time() - t0:6.0f}s {path.stat().st_size / 1e6:6.2f} MB")


if __name__ == "__main__":
    main()
