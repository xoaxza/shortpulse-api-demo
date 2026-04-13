# ShortPulse API

ShortPulse API turns FINRA Consolidated NMS daily short-sale volume files into clean, caveat-aware JSON for AI agents, trading copilots, watchlist bots, and finance assistants.

Important caveats:

- This is short-sale volume, not short interest.
- `v1` only covers the FINRA Consolidated NMS daily file.
- FINRA-reported activity is not a complete exchange-consolidated picture.
- FINRA files may be revised after publication.
- Derived metrics like `short_ratio` are convenience calculations.

## What It Exposes

- `GET /` HTML landing page and docs
- `GET /v1/status`
- `GET /v1/ticker/{symbol}/latest`
- `GET /v1/ticker/{symbol}/history?days=20`
- `GET /v1/rankings?metric=short_ratio|short_volume&limit=50&date=YYYY-MM-DD`
- `GET /v1/signals/unusual?date=YYYY-MM-DD&min_total_volume=1000000&lookback=10`
- `POST /v1/batch/lookup`

## Local Run

No third-party dependencies are required.

```bash
python3 app.py
```

The service binds to `0.0.0.0` and reads `PORT` from the environment. Defaults:

- `PORT=8000`
- `SHORTPULSE_DB_PATH=data/shortpulse.sqlite`

Example:

```bash
PORT=8000 python3 app.py
```

Open `http://127.0.0.1:8000/`.

## Fixture Mode

For deterministic local tests without external network access:

```bash
SHORTPULSE_FIXTURE_DIR=fixtures/finra python3 app.py
```

Optional test-only override:

- `SHORTPULSE_TODAY=YYYY-MM-DD`

## Endpoint Examples

```bash
curl http://127.0.0.1:8000/v1/status
```

```bash
curl http://127.0.0.1:8000/v1/ticker/TSLA/latest
```

```bash
curl "http://127.0.0.1:8000/v1/ticker/AAPL/history?days=5"
```

```bash
curl "http://127.0.0.1:8000/v1/rankings?metric=short_ratio&limit=10"
```

```bash
curl "http://127.0.0.1:8000/v1/signals/unusual?lookback=10&min_total_volume=1000000"
```

```bash
curl -X POST http://127.0.0.1:8000/v1/batch/lookup \
  -H "Content-Type: application/json" \
  -d '{"symbols":["AAPL","TSLA","GME"]}'
```

## Smoke Test

The smoke test starts the HTTP server against local fixture files and hits the real endpoints.

```bash
python3 smoke_test.py
```

## Render

This repo is ready to run as a single Python web service on Render. No build-time package install is needed.

Start command:

```bash
python3 app.py
```

Suggested environment variable:

- `SHORTPULSE_DB_PATH=data/shortpulse.sqlite`

Notes:

- The local SQLite cache is fine for a tiny single-instance deployment.
- On Render free or ephemeral disks, cache contents may reset on redeploy or restart.
- x402-ready paid access can be added later, but it is not live today.

## Implementation Notes

- FINRA source pattern: `https://cdn.finra.org/equity/regsho/daily/CNMSshvolYYYYMMDD.txt`
- Recent dates are probed to find the latest available file.
- Files are parsed as pipe-delimited text.
- Trailer rows are ignored.
- Parsed rows are cached in SQLite with one row per symbol per trade date.
- `short_ratio` is computed as `short_volume / total_volume`.
