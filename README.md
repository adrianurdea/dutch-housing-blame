# dutch-housing-blame

Who gets blamed for the Dutch housing crisis? Comparing Dutch- and English-language Reddit communities (2024–2026).

Group 20 project for *The Social Web* (VU Amsterdam, 2026).

**RQ:** Who do people blame for the Dutch housing crisis online, and do Dutch-language
(r/thenetherlands) and English-language (r/Netherlands) communities blame different actors?

## Setup
```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -r requirements-lock.txt
```
Put the shared salt for hashing usernames in a `.env` file (ask the group, don't commit it):
```
PSEUDONYM_SALT=...
```

## Data
Reddit posts and comments from 2024-01-01 to 2026-10-31, collected with the
[Arctic Shift API](https://github.com/ArthurHeitmann/arctic_shift) by `collect.py`.

The raw data contains usernames, so it is **not** on GitHub. Get it from our SURFdrive
folder and put it in `data/raw/`. To download it again yourself (takes ~12 hours, can be resumed):
```bash
python -u collect.py > crawl.log 2>&1
```

## Folders
- `notebooks/`: the final notebook (all analysis)
- `data/raw/`, `data/processed/`: data (not in git)
- `annotation/`: codebook (annotation files are not in git)
- `figures/`: plots for the paper
