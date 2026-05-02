# trend-scan

Public trend data collection and accumulation for comparing changes over time.

This repository is the public-side data base for a larger trend scanning workflow. Its job is to collect daily snapshots from open sources, normalize them into one schema, detect notable changes, and publish machine-readable history inside the repository itself.

## Goal

This project is optimized for three questions:

1. What is being talked about now?
2. What suddenly accelerated?
3. What can we compare over time later?

The public repository only handles data collection, storage, normalization, and signal detection. Any private judgment layer can be built later on top of the generated data.

## Data layers

- `RSS` -> `NEWS`
- `Hacker News` -> `REACTION`
- `GitHub` -> `BEHAVIOR`
- `Wikipedia Pageviews` -> `INTEREST`
- `Polymarket` -> `EXPECTATION`

## Repository layout

```text
.
├─ .github/workflows/daily-collection.yml
├─ config/
│  ├─ keywords.yml
│  ├─ sources.yml
│  └─ watchlists.yml
├─ data/
│  ├─ normalized/
│  ├─ raw/
│  └─ signals/
├─ reports/
│  ├─ daily/
│  └─ weekly/
├─ scripts/
│  ├─ collect_rss.py
│  ├─ collect_hackernews.py
│  ├─ collect_github.py
│  ├─ collect_wikipedia.py
│  ├─ collect_polymarket.py
│  ├─ normalize.py
│  ├─ detect_signals.py
│  ├─ generate_daily_report.py
│  └─ run_daily.py
└─ src/trend_scan/
```

## Output format

Each run writes three layers of outputs:

- `data/raw/YYYY-MM-DD/*.json`
  Raw source snapshots for later reprocessing and debugging.
- `data/normalized/YYYY-MM-DD.jsonl`
  Unified records across all sources with one shared schema.
- `data/signals/YYYY-MM-DD_signals.json`
  The subset of items that look materially different or important.

Wikipedia pageviews use the most recent fully available daily point when the pipeline runs on schedule.

Daily markdown reports are written to `reports/daily/YYYY-MM-DD.md`.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/run_daily.py
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts/run_daily.py
```

You can backfill a specific day with:

```bash
python scripts/run_daily.py --date 2026-05-02
```

## GitHub Actions schedule

The workflow runs at `06:00` Japan time every day.

- GitHub Actions cron is `0 21 * * *` in UTC.
- The pipeline writes data and report changes back into the same repository.
- GitHub API calls will use `GITHUB_TOKEN` automatically when available.

## Configuration

- `config/sources.yml`
  Source endpoints and RSS feeds.
- `config/watchlists.yml`
  Search queries, monitored Wikipedia pages, and Polymarket category filters.
- `config/keywords.yml`
  Theme tagging rules and signal-oriented terms.

The included config is a starter set for the MVP. Expand feed counts, GitHub queries, and watched pages as coverage grows.

## Data policy

- Keep raw metadata and daily snapshots.
- Do not store full article bodies.
- Do not store personal data or large comment dumps.
- Favor public APIs and feed endpoints only.

## Next steps

- Add more RSS feeds until the public news layer reaches target coverage.
- Add weekly and monthly aggregators on top of `signals`.
- Split private decision logic into a separate repository later.
