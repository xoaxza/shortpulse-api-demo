from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
import statistics
import urllib.error
import urllib.request
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable


FINRA_URL_TEMPLATE = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{yyyymmdd}.txt"
USER_AGENT = (
    "Mozilla/5.0 (compatible; ShortPulseAPI/1.0; +https://shortpulse.local)"
)
DISCLAIMER = "ShortPulse API exposes FINRA short-sale volume, not short interest."
CAVEATS = [
    DISCLAIMER,
    "v1 only covers the FINRA Consolidated NMS daily file.",
    "FINRA-reported activity is not a complete exchange-consolidated picture.",
    "FINRA files may be revised after publication.",
    "Derived metrics like short_ratio are convenience calculations.",
]
TRAILER_SYMBOLS = {"NMS", "NMSCOMPOSITE", "TOTAL"}
FIXTURE_PATTERN = re.compile(r"CNMSshvol(\d{8})\.txt$")
DEFAULT_RECENT_INGEST_TTL_SECONDS = 6 * 60 * 60
DEFAULT_RECENT_INGEST_WINDOW_DAYS = 7
DEFAULT_LATEST_PROBE_TTL_SECONDS = 15 * 60


class ShortPulseError(Exception):
    pass


class BadRequestError(ShortPulseError):
    pass


class NotFoundError(ShortPulseError):
    pass


class UpstreamError(ShortPulseError):
    pass


def utc_now_datetime() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def utc_now() -> str:
    return utc_now_datetime().isoformat().replace("+00:00", "Z")


def iso_date(value: date) -> str:
    return value.isoformat()


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise BadRequestError("Expected date in YYYY-MM-DD format.") from exc


def parse_finra_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise BadRequestError("Unexpected FINRA trade date format.") from exc


def parse_utc_timestamp(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError as exc:
        raise BadRequestError("Unexpected timestamp format.") from exc


def normalize_symbol(symbol: str) -> str:
    cleaned = (symbol or "").strip().upper()
    if not cleaned:
        raise BadRequestError("Symbol is required.")
    if not re.fullmatch(r"[A-Z0-9./\-]{1,15}", cleaned):
        raise BadRequestError("Symbol must be 1-15 chars of A-Z, 0-9, '.', '-', or '/'.")
    return cleaned


def parse_finra_numeric(value: str) -> int | float:
    cleaned = (value or "").strip()
    if not cleaned:
        return 0
    try:
        parsed = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError("Unexpected FINRA numeric value.") from exc
    if not parsed.is_finite():
        raise ValueError("Unexpected FINRA numeric value.")
    return int(parsed) if parsed == parsed.to_integral_value() else float(parsed)


def short_ratio(short_volume: int | float, total_volume: int | float) -> float | None:
    if total_volume <= 0:
        return None
    return round(short_volume / total_volume, 6)


def chunked(values: list[str], size: int = 250) -> Iterable[list[str]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


class ShortPulseService:
    def __init__(
        self,
        db_path: str | os.PathLike[str],
        fixture_dir: str | os.PathLike[str] | None = None,
        today: date | None = None,
        recent_ingest_ttl_seconds: int = DEFAULT_RECENT_INGEST_TTL_SECONDS,
        recent_ingest_window_days: int = DEFAULT_RECENT_INGEST_WINDOW_DAYS,
        latest_probe_ttl_seconds: int = DEFAULT_LATEST_PROBE_TTL_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.fixture_dir = Path(fixture_dir) if fixture_dir else None
        self._today = today
        self.recent_ingest_ttl_seconds = max(0, recent_ingest_ttl_seconds)
        self.recent_ingest_window_days = max(0, recent_ingest_window_days)
        self.latest_probe_ttl_seconds = max(0, latest_probe_ttl_seconds)
        self._clock = clock or utc_now_datetime
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS daily_short_volume (
                    trade_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    short_volume INTEGER NOT NULL,
                    short_exempt_volume INTEGER NOT NULL,
                    total_volume INTEGER NOT NULL,
                    market TEXT NOT NULL,
                    short_ratio REAL,
                    source_url TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    PRIMARY KEY (trade_date, symbol)
                );

                CREATE INDEX IF NOT EXISTS idx_daily_symbol_date
                    ON daily_short_volume(symbol, trade_date DESC);

                CREATE INDEX IF NOT EXISTS idx_daily_trade_date
                    ON daily_short_volume(trade_date DESC);

                CREATE TABLE IF NOT EXISTS ingest_runs (
                    trade_date TEXT PRIMARY KEY,
                    source_url TEXT NOT NULL,
                    source_mode TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    fetched_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS latest_trade_date_probe (
                    probe_key INTEGER PRIMARY KEY CHECK (probe_key = 1),
                    trade_date TEXT NOT NULL,
                    checked_at TEXT NOT NULL
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def today(self) -> date:
        return self._today or self.now().date()

    def now(self) -> datetime:
        current = self._clock()
        if current.tzinfo is None:
            current = current.replace(tzinfo=UTC)
        return current.astimezone(UTC).replace(microsecond=0)

    def now_iso(self) -> str:
        return self.now().isoformat().replace("+00:00", "Z")

    def source_url_for_date(self, trade_date: date) -> str:
        return FINRA_URL_TEMPLATE.format(yyyymmdd=trade_date.strftime("%Y%m%d"))

    def source_basename_for_date(self, trade_date: date) -> str:
        return Path(self.source_url_for_date(trade_date)).name

    def list_fixture_dates(self) -> list[date]:
        if not self.fixture_dir or not self.fixture_dir.exists():
            return []
        dates: list[date] = []
        for path in self.fixture_dir.iterdir():
            match = FIXTURE_PATTERN.match(path.name)
            if not match:
                continue
            dates.append(parse_finra_date(match.group(1)))
        return sorted(dates, reverse=True)

    def fetch_raw_text(
        self, trade_date: date, treat_403_as_missing: bool = False
    ) -> tuple[str, str, str] | None:
        source_url = self.source_url_for_date(trade_date)
        if self.fixture_dir:
            fixture_path = self.fixture_dir / self.source_basename_for_date(trade_date)
            if not fixture_path.exists():
                return None
            return (
                source_url,
                fixture_path.read_text(encoding="utf-8"),
                "fixture",
            )

        request = urllib.request.Request(
            source_url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/plain, */*",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return (
                    source_url,
                    response.read().decode("utf-8"),
                    "network",
                )
        except urllib.error.HTTPError as exc:
            if exc.code == 404 or (treat_403_as_missing and exc.code == 403):
                return None
            raise UpstreamError(f"FINRA fetch failed with HTTP {exc.code}.") from exc
        except urllib.error.URLError as exc:
            raise UpstreamError(f"FINRA fetch failed: {exc.reason}") from exc

    def parse_rows(self, raw_text: str, source_url: str) -> list[dict]:
        reader = csv.reader(io.StringIO(raw_text), delimiter="|")
        rows = list(reader)
        if not rows:
            return []

        header = [column.strip().lower() for column in rows[0]]
        required = {
            "date": "date",
            "symbol": "symbol",
            "shortvolume": "shortvolume",
            "shortexemptvolume": "shortexemptvolume",
            "totalvolume": "totalvolume",
            "market": "market",
        }
        try:
            indices = {target: header.index(source) for source, target in required.items()}
        except ValueError as exc:
            raise UpstreamError(f"Unexpected FINRA header in {source_url}.") from exc

        parsed: list[dict] = []
        ingested_at = self.now_iso()
        for row in rows[1:]:
            if not row or all(not cell.strip() for cell in row):
                continue
            if len(row) < len(header):
                continue
            trade_date = row[indices["date"]].strip()
            symbol = row[indices["symbol"]].strip().upper()
            market = row[indices["market"]].strip().upper()
            if self.is_trailer_row(symbol=symbol, market=market):
                continue
            try:
                short_volume = parse_finra_numeric(row[indices["shortvolume"]])
                short_exempt_volume = parse_finra_numeric(
                    row[indices["shortexemptvolume"]]
                )
                total_volume = parse_finra_numeric(row[indices["totalvolume"]])
            except ValueError:
                continue
            try:
                normalized_symbol = normalize_symbol(symbol)
            except BadRequestError:
                continue

            parsed.append(
                {
                    "trade_date": iso_date(parse_finra_date(trade_date)),
                    "symbol": normalized_symbol,
                    "short_volume": short_volume,
                    "short_exempt_volume": short_exempt_volume,
                    "total_volume": total_volume,
                    "market": market or "N/A",
                    "short_ratio": short_ratio(short_volume, total_volume),
                    "source_url": source_url,
                    "ingested_at": ingested_at,
                }
            )
        return parsed

    @staticmethod
    def is_trailer_row(symbol: str, market: str) -> bool:
        return symbol in TRAILER_SYMBOLS and not market

    def get_cached_ingest(self, trade_date: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT trade_date, source_url, source_mode, row_count, fetched_at
                FROM ingest_runs
                WHERE trade_date = ?
                """,
                (trade_date,),
            ).fetchone()

    def get_latest_trade_date_probe(self) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT trade_date, checked_at
                FROM latest_trade_date_probe
                WHERE probe_key = 1
                """
            ).fetchone()

    def set_latest_trade_date_probe(
        self, trade_date: str, checked_at: str | None = None
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO latest_trade_date_probe (
                    probe_key,
                    trade_date,
                    checked_at
                ) VALUES (1, ?, ?)
                """,
                (trade_date, checked_at or self.now_iso()),
            )

    def latest_cached_ingest(self) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT trade_date, source_url, source_mode, row_count, fetched_at
                FROM ingest_runs
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ).fetchone()

    def cache_expired(self, fetched_at: str, ttl_seconds: int) -> bool:
        if ttl_seconds <= 0:
            return True
        age = self.now() - parse_utc_timestamp(fetched_at)
        return age.total_seconds() >= ttl_seconds

    def should_refresh_ingest(self, trade_date: date, cached: sqlite3.Row) -> bool:
        age_days = (self.today() - trade_date).days
        if age_days > self.recent_ingest_window_days:
            return False
        return self.cache_expired(cached["fetched_at"], self.recent_ingest_ttl_seconds)

    def should_refresh_latest_probe(self, cached: sqlite3.Row) -> bool:
        return self.cache_expired(cached["checked_at"], self.latest_probe_ttl_seconds)

    def ingest_trade_date(
        self, trade_date: date, treat_403_as_missing: bool = False
    ) -> dict | None:
        trade_date_iso = iso_date(trade_date)
        cached = self.get_cached_ingest(trade_date_iso)
        if cached and not self.should_refresh_ingest(trade_date, cached):
            return dict(cached)

        fetched = self.fetch_raw_text(
            trade_date, treat_403_as_missing=treat_403_as_missing
        )
        if not fetched:
            return dict(cached) if cached else None
        source_url, raw_text, source_mode = fetched
        parsed_rows = self.parse_rows(raw_text, source_url)
        if not parsed_rows:
            return dict(cached) if cached else None

        fetched_at = self.now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM daily_short_volume
                WHERE trade_date = ?
                """,
                (trade_date_iso,),
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO daily_short_volume (
                    trade_date,
                    symbol,
                    short_volume,
                    short_exempt_volume,
                    total_volume,
                    market,
                    short_ratio,
                    source_url,
                    ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["trade_date"],
                        row["symbol"],
                        row["short_volume"],
                        row["short_exempt_volume"],
                        row["total_volume"],
                        row["market"],
                        row["short_ratio"],
                        row["source_url"],
                        row["ingested_at"],
                    )
                    for row in parsed_rows
                ],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO ingest_runs (
                    trade_date,
                    source_url,
                    source_mode,
                    row_count,
                    fetched_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    trade_date_iso,
                    source_url,
                    source_mode,
                    len(parsed_rows),
                    fetched_at,
                ),
            )

        payload = {
            "trade_date": trade_date_iso,
            "source_url": source_url,
            "source_mode": source_mode,
            "row_count": len(parsed_rows),
            "fetched_at": fetched_at,
        }
        latest_probe = self.get_latest_trade_date_probe()
        if not latest_probe or trade_date_iso >= latest_probe["trade_date"]:
            self.set_latest_trade_date_probe(trade_date_iso, checked_at=fetched_at)
        return payload

    def ensure_recent_trade_dates(
        self, count: int, end_date: str | None = None, max_probe_days: int | None = None
    ) -> list[str]:
        if count <= 0:
            return []
        if self.fixture_dir:
            candidates = self.list_fixture_dates()
            if end_date:
                cutoff = parse_iso_date(end_date)
                candidates = [item for item in candidates if item <= cutoff]
            for trade_date in candidates[:count]:
                self.ingest_trade_date(trade_date)
            return [iso_date(item) for item in candidates[:count]]

        if end_date:
            anchor = parse_iso_date(end_date)
        else:
            anchor = parse_iso_date(self.latest_available_trade_date())
        probe_days = max_probe_days or max(30, count * 4)
        found: list[str] = []
        for offset in range(probe_days):
            candidate = anchor - timedelta(days=offset)
            ingested = self.ingest_trade_date(candidate, treat_403_as_missing=True)
            if ingested:
                trade_date_iso = ingested["trade_date"]
                if trade_date_iso not in found:
                    found.append(trade_date_iso)
                if len(found) >= count:
                    break
        return found

    def latest_available_trade_date(self) -> str:
        cached_probe = self.get_latest_trade_date_probe()
        if cached_probe and not self.should_refresh_latest_probe(cached_probe):
            return cached_probe["trade_date"]

        if self.fixture_dir:
            dates = self.ensure_recent_trade_dates(1)
            if not dates:
                raise NotFoundError("No FINRA daily short-sale volume file is available yet.")
            self.set_latest_trade_date_probe(dates[0])
            return dates[0]

        latest_cached = self.latest_cached_ingest()
        probe_days = 30
        if latest_cached:
            probe_days = max(
                probe_days,
                (self.today() - parse_iso_date(latest_cached["trade_date"])).days + 1,
            )

        for offset in range(probe_days):
            candidate = self.today() - timedelta(days=offset)
            ingested = self.ingest_trade_date(candidate, treat_403_as_missing=True)
            if ingested:
                trade_date_iso = ingested["trade_date"]
                self.set_latest_trade_date_probe(trade_date_iso)
                return trade_date_iso

        raise NotFoundError("No FINRA daily short-sale volume file is available yet.")

    def require_trade_date(self, requested_date: str | None = None) -> str:
        if requested_date:
            trade_date = iso_date(parse_iso_date(requested_date))
            ingested = self.ingest_trade_date(
                parse_iso_date(requested_date),
                treat_403_as_missing=True,
            )
            if not ingested:
                raise NotFoundError(f"No FINRA file found for {trade_date}.")
            return trade_date
        return self.latest_available_trade_date()

    def serialize_row(self, row: sqlite3.Row | dict) -> dict:
        payload = dict(row)
        payload["short_ratio"] = (
            None if payload.get("short_ratio") is None else round(payload["short_ratio"], 6)
        )
        return payload

    def status(self) -> dict:
        latest_trade_date = self.latest_available_trade_date()
        latest_probe = self.get_latest_trade_date_probe()
        with self._connect() as conn:
            summary = conn.execute(
                """
                SELECT COUNT(*) AS total_rows, COUNT(DISTINCT trade_date) AS cached_trade_dates
                FROM daily_short_volume
                """
            ).fetchone()
            latest_ingest = conn.execute(
                """
                SELECT trade_date, source_url, source_mode, row_count, fetched_at
                FROM ingest_runs
                WHERE row_count > 0
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ).fetchone()
            recent_sources = conn.execute(
                """
                SELECT source_url
                FROM ingest_runs
                WHERE row_count > 0
                ORDER BY trade_date DESC
                LIMIT 5
                """
            ).fetchall()

        lag_days = (self.today() - parse_iso_date(latest_trade_date)).days
        freshness = "fresh" if lag_days <= 3 else "stale"
        return {
            "service": "ShortPulse API",
            "latest_trade_date": latest_trade_date,
            "latest_row_count": latest_ingest["row_count"] if latest_ingest else 0,
            "source_urls": [row["source_url"] for row in recent_sources],
            "cache": {
                "db_path": str(self.db_path),
                "cached_trade_dates": summary["cached_trade_dates"],
                "cached_rows": summary["total_rows"],
                "latest_source_mode": latest_ingest["source_mode"] if latest_ingest else None,
                "last_ingested_at": latest_ingest["fetched_at"] if latest_ingest else None,
                "latest_trade_date_probe": {
                    "trade_date": latest_probe["trade_date"] if latest_probe else None,
                    "checked_at": latest_probe["checked_at"] if latest_probe else None,
                    "ttl_seconds": self.latest_probe_ttl_seconds,
                },
                "refresh_policy": {
                    "recent_ingest_window_days": self.recent_ingest_window_days,
                    "recent_ingest_ttl_seconds": self.recent_ingest_ttl_seconds,
                    "latest_probe_ttl_seconds": self.latest_probe_ttl_seconds,
                },
                "freshness": freshness,
                "calendar_days_behind": lag_days,
            },
        }

    def latest_for_symbol(self, symbol: str) -> dict:
        symbol = normalize_symbol(symbol)
        self.ensure_recent_trade_dates(30)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT trade_date, symbol, short_volume, short_exempt_volume, total_volume,
                       market, short_ratio, source_url, ingested_at
                FROM daily_short_volume
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
        if not row:
            raise NotFoundError(f"No cached short-sale volume data found for {symbol}.")
        return self.serialize_row(row)

    def history_for_symbol(self, symbol: str, days: int) -> dict:
        symbol = normalize_symbol(symbol)
        if days <= 0 or days > 365:
            raise BadRequestError("days must be between 1 and 365.")
        self.ensure_recent_trade_dates(min(max(days * 3, days + 5), 365))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT trade_date, symbol, short_volume, short_exempt_volume, total_volume,
                       market, short_ratio, source_url, ingested_at
                FROM daily_short_volume
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (symbol, days),
            ).fetchall()
        if not rows:
            raise NotFoundError(f"No cached short-sale volume history found for {symbol}.")
        history = [self.serialize_row(row) for row in reversed(rows)]
        return {
            "symbol": symbol,
            "requested_days": days,
            "returned_days": len(history),
            "history": history,
        }

    def rankings(self, metric: str, limit: int, requested_date: str | None) -> dict:
        if metric not in {"short_ratio", "short_volume"}:
            raise BadRequestError("metric must be short_ratio or short_volume.")
        if limit <= 0 or limit > 200:
            raise BadRequestError("limit must be between 1 and 200.")
        trade_date = self.require_trade_date(requested_date)
        order_clause = (
            "short_ratio DESC, short_volume DESC, total_volume DESC, symbol ASC"
            if metric == "short_ratio"
            else "short_volume DESC, total_volume DESC, symbol ASC"
        )
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT trade_date, symbol, short_volume, short_exempt_volume, total_volume,
                       market, short_ratio, source_url, ingested_at
                FROM daily_short_volume
                WHERE trade_date = ? AND total_volume > 0
                ORDER BY {order_clause}
                LIMIT ?
                """,
                (trade_date, limit),
            ).fetchall()
        return {
            "trade_date": trade_date,
            "metric": metric,
            "limit": limit,
            "results": [self.serialize_row(row) for row in rows],
        }

    def unusual_signals(
        self,
        requested_date: str | None,
        min_total_volume: int,
        lookback: int,
    ) -> dict:
        if min_total_volume < 0:
            raise BadRequestError("min_total_volume must be non-negative.")
        if lookback <= 1 or lookback > 60:
            raise BadRequestError("lookback must be between 2 and 60.")

        trade_date = self.require_trade_date(requested_date)
        trade_dates = self.ensure_recent_trade_dates(lookback + 1, end_date=trade_date)
        previous_dates = [item for item in trade_dates if item < trade_date][:lookback]
        if len(previous_dates) < 2:
            return {
                "trade_date": trade_date,
                "lookback": lookback,
                "min_total_volume": min_total_volume,
                "method": "spike_vs_average",
                "results": [],
            }

        with self._connect() as conn:
            current_rows = conn.execute(
                """
                SELECT trade_date, symbol, short_volume, short_exempt_volume, total_volume,
                       market, short_ratio, source_url, ingested_at
                FROM daily_short_volume
                WHERE trade_date = ? AND total_volume >= ?
                ORDER BY short_volume DESC
                """,
                (trade_date, min_total_volume),
            ).fetchall()

            if not current_rows:
                return {
                    "trade_date": trade_date,
                    "lookback": lookback,
                    "min_total_volume": min_total_volume,
                    "method": "spike_vs_average",
                    "results": [],
                }

            symbols = [row["symbol"] for row in current_rows]
            historical_rows: list[sqlite3.Row] = []
            date_placeholders = ",".join("?" for _ in previous_dates)
            for symbol_group in chunked(symbols):
                symbol_placeholders = ",".join("?" for _ in symbol_group)
                historical_rows.extend(
                    conn.execute(
                        f"""
                        SELECT symbol, short_volume, short_ratio
                        FROM daily_short_volume
                        WHERE trade_date IN ({date_placeholders})
                          AND symbol IN ({symbol_placeholders})
                        """,
                        tuple(previous_dates + symbol_group),
                    ).fetchall()
                )

        history_map: dict[str, dict[str, list[float]]] = defaultdict(
            lambda: {"short_volume": [], "short_ratio": []}
        )
        for row in historical_rows:
            history_map[row["symbol"]]["short_volume"].append(float(row["short_volume"]))
            if row["short_ratio"] is not None:
                history_map[row["symbol"]]["short_ratio"].append(float(row["short_ratio"]))

        signals: list[dict] = []
        minimum_samples = min(3, lookback)
        for row in current_rows:
            series = history_map.get(row["symbol"])
            if not series or len(series["short_volume"]) < minimum_samples:
                continue

            avg_short_volume = sum(series["short_volume"]) / len(series["short_volume"])
            avg_short_ratio = (
                sum(series["short_ratio"]) / len(series["short_ratio"])
                if series["short_ratio"]
                else None
            )
            current_short_volume = float(row["short_volume"])
            volume_spike = round(current_short_volume / avg_short_volume, 3) if avg_short_volume else None
            ratio_delta = (
                round(float(row["short_ratio"]) - avg_short_ratio, 6)
                if row["short_ratio"] is not None and avg_short_ratio is not None
                else None
            )

            z_score = None
            if len(series["short_volume"]) >= 2:
                stddev = statistics.pstdev(series["short_volume"])
                if stddev > 0:
                    z_score = round((current_short_volume - avg_short_volume) / stddev, 3)

            is_unusual = (
                volume_spike is not None
                and volume_spike >= 2.0
                and current_short_volume > avg_short_volume
            ) or (z_score is not None and z_score >= 2.0)
            if not is_unusual:
                continue

            entry = self.serialize_row(row)
            entry.update(
                {
                    "baseline_short_volume_avg": round(avg_short_volume, 2),
                    "baseline_short_ratio_avg": (
                        None if avg_short_ratio is None else round(avg_short_ratio, 6)
                    ),
                    "lookback_samples": len(series["short_volume"]),
                    "volume_spike": volume_spike,
                    "short_volume_zscore": z_score,
                    "short_ratio_delta": ratio_delta,
                }
            )
            signals.append(entry)

        signals.sort(
            key=lambda item: (
                float("-inf")
                if item["short_volume_zscore"] is None
                else item["short_volume_zscore"],
                item["volume_spike"] or 0,
                item["short_volume"],
            ),
            reverse=True,
        )
        return {
            "trade_date": trade_date,
            "lookback": lookback,
            "min_total_volume": min_total_volume,
            "method": "spike_vs_average",
            "results": signals[:50],
        }

    def batch_lookup(self, symbols: list[str]) -> dict:
        if not isinstance(symbols, list):
            raise BadRequestError("symbols must be an array of ticker strings.")
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_symbol in symbols[:200]:
            symbol = normalize_symbol(str(raw_symbol))
            if symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
        if not normalized:
            raise BadRequestError("symbols must contain at least one valid ticker.")

        recent_dates = self.ensure_recent_trade_dates(30)
        if not recent_dates:
            raise NotFoundError("No FINRA daily short-sale volume file is available yet.")

        rows_by_symbol: dict[str, dict] = {}
        with self._connect() as conn:
            date_placeholders = ",".join("?" for _ in recent_dates)
            for symbol_group in chunked(normalized):
                symbol_placeholders = ",".join("?" for _ in symbol_group)
                rows = conn.execute(
                    f"""
                    SELECT trade_date, symbol, short_volume, short_exempt_volume, total_volume,
                           market, short_ratio, source_url, ingested_at
                    FROM daily_short_volume
                    WHERE trade_date IN ({date_placeholders})
                      AND symbol IN ({symbol_placeholders})
                    ORDER BY symbol ASC, trade_date DESC
                    """,
                    tuple(recent_dates + symbol_group),
                ).fetchall()
                for row in rows:
                    if row["symbol"] not in rows_by_symbol:
                        rows_by_symbol[row["symbol"]] = self.serialize_row(row)

        return {
            "requested_symbols": normalized,
            "found": [rows_by_symbol[symbol] for symbol in normalized if symbol in rows_by_symbol],
            "missing_symbols": [symbol for symbol in normalized if symbol not in rows_by_symbol],
        }


def json_dumps(payload: dict) -> bytes:
    return json.dumps(payload, indent=2, sort_keys=False).encode("utf-8")
