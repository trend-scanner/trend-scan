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
|-- .github/workflows/daily-collection.yml
|-- config/
|   |-- keywords.yml
|   |-- sources.yml
|   `-- watchlists.yml
|-- data/
|   |-- errors/
|   |-- normalized/
|   |-- raw/
|   `-- signals/
|-- reports/
|   |-- daily/
|   |-- weekly/
|   `-- monthly/
|-- scripts/
|   |-- collect_rss.py
|   |-- collect_hackernews.py
|   |-- collect_github.py
|   |-- collect_wikipedia.py
|   |-- collect_polymarket.py
|   |-- normalize.py
|   |-- log_errors.py
|   |-- detect_signals.py
|   |-- generate_daily_report.py
|   |-- generate_periodic_report.py
|   |-- run_daily.py
|   |-- run_weekly.py
|   |-- run_monthly.py
|   `-- run_private_daily.py
`-- src/trend_scan/
```

## Output format

Each run writes three layers of outputs:

- `data/raw/YYYY-MM-DD/*.json`
  Raw source snapshots for later reprocessing and debugging.
- `data/normalized/YYYY-MM-DD.jsonl`
  Unified records across all sources with one shared schema.
- `data/signals/YYYY-MM-DD_signals.json`
  Detected movement signals. The file keeps all signals plus `important_signals` and `top_signals` for report display.
- `data/errors/YYYY-MM-DD_errors.json`
  Source errors and warnings for that run.
- `data/errors/error_state.json`
  Active and resolved source problems, including consecutive failure counts.

Signal ranking favors movement over size: first-seen items, recent acceleration, GitHub star/fork deltas, cross-source confirmation, and global-first Japan gaps rank above static incumbents. Polymarket is filtered through a strategy-relevant watchlist and excludes sports, entertainment, and election-horse-race noise by default.

Wikipedia pageviews try recent daily points and use the newest available date, because Wikimedia daily data can lag behind the scheduled run.

Daily markdown reports are written to `reports/daily/YYYY-MM-DD.md` and show collection health near the top so missing data is not mistaken for lack of interest.
Weekly reports are written to `reports/weekly/YYYY-Www.md`.
Monthly reports are written to `reports/monthly/YYYY-MM.md` and summarize the trailing 30-day window ending on the run date.

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

Generate periodic reports with:

```bash
python scripts/run_weekly.py --date 2026-05-02
python scripts/run_monthly.py --date 2026-05-02
```

## Private local sources

Some feeds are useful for personal monitoring but should not be committed into this public repository. For those, copy `config/private_sources.example.yml` to `config/private_sources.yml` and run:

```bash
python scripts/run_private_daily.py
```

Private outputs are written under `data/private/`, which is ignored by git. The starter private config includes Nikkei Asia RSS for personal, noncommercial reading.

## GitHub Actions schedule

The workflow runs at `06:00` Japan time every day.

- GitHub Actions cron is `0 21 * * *` in UTC.
- The pipeline writes data and report changes back into the same repository.
- GitHub API calls will use `GITHUB_TOKEN` automatically when available.
- Weekly reports run every Monday at `06:30` Japan time.
- Monthly reports run on the first day of each month at `07:00` Japan time.

## Configuration

- `config/sources.yml`
  Source endpoints, RSS feeds, Wikipedia lag settings, and signal display limits.
- `config/watchlists.yml`
  Search queries, monitored Wikipedia pages, and Polymarket include/exclude filters.
- `config/keywords.yml`
  Theme tagging rules and signal-oriented terms.

The included config is a starter set for the MVP. Expand feed counts, GitHub queries, and watched pages as coverage grows.

## Data policy

- Keep raw metadata and daily snapshots.
- Do not store full article bodies.
- Do not store personal data or large comment dumps.
- Favor public APIs and feed endpoints only.

## Next steps

- Tune signal thresholds as more history accumulates.
- Add global-vs-Japan gap reporting on weekly and monthly windows.
- Split private decision logic into a separate repository later.
