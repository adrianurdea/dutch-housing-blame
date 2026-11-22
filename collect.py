"""Download all posts and comments of r/thenetherlands and r/Netherlands from the
Arctic Shift API, one file per week (data/raw/<kind>/<subreddit>/<week>.jsonl.zst).

Weeks that are already downloaded are skipped, so the script can be restarted.
Example: python collect.py --start 2024-01-01 --end 2026-11-01
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

COMMON_FIELDS = ["id", "author", "author_flair_text", "created_utc", "subreddit", "score",
                 "distinguished", "retrieved_on"]
FIELDS = {
    "posts": COMMON_FIELDS + ["title", "selftext", "num_comments", "link_flair_text", "url", "over_18"],
    "comments": COMMON_FIELDS + ["body", "link_id", "parent_id"],
}

RAW_DIR = Path("data/raw")
FAILURE_LOG = RAW_DIR / "failures.csv"
MIN_INTERVAL = 1.0  # max 1 request per second
MAX_RETRIES = 6
MIN_PAGE = 100

_last_request = 0.0


def get_page(kind, subreddit, after, before):
    global _last_request
    params = {"subreddit": subreddit, "after": after, "before": before, "sort": "asc",
              "limit": "auto", "fields": ",".join(FIELDS[kind])}
    for attempt in range(1, MAX_RETRIES + 1):
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
            reset = r.headers.get("X-RateLimit-Reset") if r.status_code == 429 else None
        # on a rate limit wait until the reset, otherwise retry after 5, 10, 20, ... s
        delay = float(reset) + 1 if reset else 5 * 2 ** (attempt - 1)
        print(f"    retry {attempt}/{MAX_RETRIES} in {delay:.0f}s ({error})")
        time.sleep(delay)
    raise RuntimeError(error)


def crawl_window(kind, subreddit, start, end):
    items, seen = [], set()
    cursor = start - 1
    while True:
        page = get_page(kind, subreddit, cursor, end)
        new = [x for x in page if x["id"] not in seen]
        seen.update(x["id"] for x in new)
        items.extend(new)
        if len(page) < MIN_PAGE:
            return items
        # go back one second so posts with the same timestamp are not skipped
        last_ts = page[-1]["created_utc"]
        cursor = last_ts - 1 if new and last_ts - 1 > cursor else last_ts


def save_jsonl_zst(items, path):
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
    d = start
    while d < end:
        yield d, min(d + dt.timedelta(days=7), end)
        d += dt.timedelta(days=7)


def to_epoch(d):
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.UTC).timestamp())


def main():
    p = argparse.ArgumentParser(description="Download Reddit data from Arctic Shift per week.")
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--end", default="2026-11-01", help="not included")
    p.add_argument("--subreddits", nargs="+", default=SUBREDDITS)
    p.add_argument("--kinds", nargs="+", default=KINDS, choices=KINDS)
    p.add_argument("--weeks", nargs="+", help="only download weeks starting on these days")
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
