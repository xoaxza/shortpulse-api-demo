from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from shortpulse import (
    CAVEATS,
    DISCLAIMER,
    BadRequestError,
    DEFAULT_LATEST_PROBE_TTL_SECONDS,
    DEFAULT_RECENT_INGEST_TTL_SECONDS,
    DEFAULT_RECENT_INGEST_WINDOW_DAYS,
    NotFoundError,
    ShortPulseService,
    UpstreamError,
    json_dumps,
    parse_iso_date,
    utc_now,
)


def int_arg(query: dict[str, list[str]], name: str, default: int) -> int:
    raw_value = query.get(name, [str(default)])[0]
    try:
        return int(raw_value)
    except ValueError as exc:
        raise BadRequestError(f"{name} must be an integer.") from exc


def str_arg(query: dict[str, list[str]], name: str, default: str | None = None) -> str | None:
    values = query.get(name)
    if not values:
        return default
    return values[0]


def env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value in {None, ""}:
        return default
    try:
        parsed = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer.") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be non-negative.")
    return parsed


def landing_page() -> str:
    endpoint_rows = [
        ("GET", "/healthz", "Lightweight health check for deploy platforms and smoke tests."),
        ("GET", "/v1/status", "Latest ingested trade date, cache freshness, and source URLs."),
        ("GET", "/v1/ticker/{symbol}/latest", "Most recent cached short-sale volume snapshot for one ticker."),
        ("GET", "/v1/ticker/{symbol}/history?days=20", "Recent history for a ticker, returned oldest to newest."),
        (
            "GET",
            "/v1/rankings?metric=short_ratio|short_volume&limit=50&date=YYYY-MM-DD",
            "Top names for one trade date.",
        ),
        (
            "GET",
            "/v1/signals/unusual?date=YYYY-MM-DD&min_total_volume=1000000&lookback=10",
            "Simple spike-vs-average unusual activity scan.",
        ),
        ("POST", "/v1/batch/lookup", "Batch latest lookup for watchlists and agent toolchains."),
    ]
    rows_html = "\n".join(
        f"<tr><td>{method}</td><td><code>{path}</code></td><td>{description}</td></tr>"
        for method, path, description in endpoint_rows
    )
    caveats_html = "".join(f"<li>{item}</li>" for item in CAVEATS)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ShortPulse API</title>
  <style>
    :root {{
      --bg: #f6f0e5;
      --panel: rgba(255, 252, 246, 0.86);
      --ink: #10243d;
      --muted: #5b6978;
      --accent: #b84b2e;
      --accent-soft: #f0b98e;
      --line: rgba(16, 36, 61, 0.12);
      --code-bg: #12253a;
      --code-ink: #f9efe5;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font: 16px/1.6 "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      background:
        radial-gradient(circle at top left, rgba(240, 185, 142, 0.55), transparent 34%),
        radial-gradient(circle at 88% 12%, rgba(184, 75, 46, 0.18), transparent 25%),
        linear-gradient(180deg, #f8f5ef 0%, var(--bg) 100%);
      min-height: 100vh;
    }}
    main {{
      max-width: 1080px;
      margin: 0 auto;
      padding: 48px 20px 80px;
    }}
    .hero {{
      display: grid;
      gap: 20px;
      grid-template-columns: 1.2fr 0.8fr;
      align-items: start;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 24px;
      backdrop-filter: blur(10px);
      box-shadow: 0 12px 30px rgba(16, 36, 61, 0.06);
    }}
    h1, h2 {{
      margin: 0 0 12px;
      line-height: 1.05;
    }}
    h1 {{
      font-size: clamp(2.5rem, 7vw, 4.8rem);
      letter-spacing: -0.04em;
      max-width: 10ch;
    }}
    h2 {{
      font-size: 1.45rem;
    }}
    p {{ margin: 0 0 14px; color: var(--muted); }}
    .eyebrow {{
      display: inline-block;
      margin-bottom: 14px;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(184, 75, 46, 0.08);
      color: var(--accent);
      font: 700 12px/1 ui-monospace, "SFMono-Regular", "SF Mono", Consolas, monospace;
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }}
    .lead {{
      color: var(--ink);
      font-size: 1.08rem;
      max-width: 58ch;
    }}
    .grid {{
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      margin-top: 18px;
    }}
    ul {{
      margin: 12px 0 0 20px;
      padding: 0;
      color: var(--muted);
    }}
    code, pre {{
      font: 13px/1.5 ui-monospace, "SFMono-Regular", "SF Mono", Consolas, monospace;
    }}
    code {{
      background: rgba(16, 36, 61, 0.06);
      border-radius: 6px;
      padding: 2px 6px;
      color: var(--ink);
    }}
    pre {{
      margin: 0;
      padding: 18px;
      background: var(--code-bg);
      color: var(--code-ink);
      border-radius: 16px;
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.96rem;
    }}
    th, td {{
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--ink);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .notice {{
      border-left: 4px solid var(--accent);
      padding-left: 16px;
      color: var(--ink);
    }}
    .tiny {{
      font-size: 0.92rem;
      color: var(--muted);
    }}
    @media (max-width: 840px) {{
      .hero, .grid {{
        grid-template-columns: 1fr;
      }}
      main {{
        padding-top: 28px;
      }}
      .card {{
        border-radius: 18px;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="card">
        <span class="eyebrow">ShortPulse API</span>
        <h1>FINRA short-sale volume, cleaned up for agents.</h1>
        <p class="lead">ShortPulse turns FINRA Consolidated NMS daily short-sale volume files into clean, caveat-aware JSON for AI agents, trading copilots, watchlist bots, and finance assistants.</p>
        <p>This is a thin, deployable v1 focused on one raw source: the FINRA Consolidated NMS file. It probes recent dates, caches parsed rows in SQLite, and exposes simple lookup, ranking, history, unusual-activity, and batch endpoints.</p>
        <div class="grid">
          <div>
            <h2>Use Cases</h2>
            <ul>
              <li>Symbol lookup for assistants and copilots</li>
              <li>Daily rankings by short ratio or short volume</li>
              <li>Ticker history for charts and watchlists</li>
              <li>Unusual activity scans for bots and alerts</li>
              <li>Batch lookup for watchlist enrichment</li>
            </ul>
          </div>
          <div>
            <h2>Roadmap</h2>
            <ul>
              <li>x402-ready paid access can come later</li>
              <li>Not live today</li>
              <li>v1 stays intentionally narrow and caveat-heavy</li>
            </ul>
          </div>
        </div>
      </div>
      <div class="card">
        <h2>Example</h2>
        <pre>curl "$BASE_URL/v1/ticker/TSLA/latest"

curl "$BASE_URL/v1/rankings?metric=short_ratio&amp;limit=10"

curl "$BASE_URL/v1/signals/unusual?lookback=10&amp;min_total_volume=1000000"

curl -X POST "$BASE_URL/v1/batch/lookup" \\
  -H "Content-Type: application/json" \\
  -d '{{"symbols":["AAPL","TSLA","GME"]}}'</pre>
      </div>
    </section>

    <section class="grid" style="margin-top: 22px;">
      <div class="card">
        <h2>Important Caveats</h2>
        <p class="notice">{DISCLAIMER}</p>
        <ul>{caveats_html}</ul>
      </div>
      <div class="card">
        <h2>Response Shape</h2>
        <p>Every JSON response includes a top-level disclaimer and caveat list so downstream agents do not confuse short-sale volume with short interest.</p>
        <pre>{{
  "ok": true,
  "disclaimer": "{DISCLAIMER}",
  "caveats": [...],
  "data": {{ ... }}
}}</pre>
      </div>
    </section>

    <section class="card" style="margin-top: 22px;">
      <h2>Endpoints</h2>
      <table>
        <thead>
          <tr><th>Method</th><th>Path</th><th>Notes</th></tr>
        </thead>
        <tbody>
          {rows_html}
        </tbody>
      </table>
      <p class="tiny">All rankings and signals are convenience views on FINRA raw files. Use them as prompts for further investigation, not as standalone market truth.</p>
    </section>
  </main>
</body>
</html>"""


class ShortPulseApplication:
    def __init__(self, service: ShortPulseService) -> None:
        self.service = service

    def json_response(
        self,
        status: HTTPStatus,
        data: dict | None = None,
        error: str | None = None,
    ) -> tuple[HTTPStatus, str, bytes, dict[str, str]]:
        payload = {
            "ok": error is None,
            "service": "ShortPulse API",
            "version": "v1",
            "generated_at": utc_now(),
            "disclaimer": DISCLAIMER,
            "caveats": CAVEATS,
        }
        if error is not None:
            payload["error"] = error
        else:
            payload["data"] = data
        return (
            status,
            "application/json; charset=utf-8",
            json_dumps(payload),
            {"Access-Control-Allow-Origin": "*"},
        )

    def html_response(self, status: HTTPStatus, html: str) -> tuple[HTTPStatus, str, bytes, dict[str, str]]:
        return (
            status,
            "text/html; charset=utf-8",
            html.encode("utf-8"),
            {"Access-Control-Allow-Origin": "*"},
        )

    def text_response(
        self, status: HTTPStatus, text: str
    ) -> tuple[HTTPStatus, str, bytes, dict[str, str]]:
        return (
            status,
            "text/plain; charset=utf-8",
            text.encode("utf-8"),
            {"Access-Control-Allow-Origin": "*"},
        )

    def empty_response(self, status: HTTPStatus) -> tuple[HTTPStatus, str, bytes, dict[str, str]]:
        return (
            status,
            "text/plain; charset=utf-8",
            b"",
            {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            },
        )

    def handle(
        self,
        method: str,
        raw_path: str,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> tuple[HTTPStatus, str, bytes, dict[str, str]]:
        del headers
        if method == "OPTIONS":
            return self.empty_response(HTTPStatus.NO_CONTENT)
        try:
            if method == "GET":
                return self.handle_get(raw_path)
            if method == "POST":
                return self.handle_post(raw_path, body)
            return self.json_response(HTTPStatus.METHOD_NOT_ALLOWED, error="Method not allowed.")
        except BadRequestError as exc:
            return self.json_response(HTTPStatus.BAD_REQUEST, error=str(exc))
        except NotFoundError as exc:
            return self.json_response(HTTPStatus.NOT_FOUND, error=str(exc))
        except UpstreamError as exc:
            return self.json_response(HTTPStatus.BAD_GATEWAY, error=str(exc))
        except Exception:  # pragma: no cover - last-resort guardrail
            return self.json_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                error="Internal server error.",
            )

    def handle_get(self, raw_path: str) -> tuple[HTTPStatus, str, bytes, dict[str, str]]:
        parsed = urlsplit(raw_path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            return self.html_response(HTTPStatus.OK, landing_page())
        if path == "/healthz":
            return self.text_response(HTTPStatus.OK, "ok\n")
        if path == "/v1/status":
            return self.json_response(HTTPStatus.OK, self.service.status())
        if path.startswith("/v1/ticker/") and path.endswith("/latest"):
            parts = [part for part in path.split("/") if part]
            if len(parts) != 4:
                raise NotFoundError("Endpoint not found.")
            return self.json_response(
                HTTPStatus.OK,
                self.service.latest_for_symbol(unquote(parts[2])),
            )
        if path.startswith("/v1/ticker/") and path.endswith("/history"):
            parts = [part for part in path.split("/") if part]
            if len(parts) != 4:
                raise NotFoundError("Endpoint not found.")
            days = int_arg(query, "days", 20)
            return self.json_response(
                HTTPStatus.OK,
                self.service.history_for_symbol(unquote(parts[2]), days),
            )
        if path == "/v1/rankings":
            metric = str_arg(query, "metric", "short_ratio")
            limit = int_arg(query, "limit", 50)
            requested_date = str_arg(query, "date")
            return self.json_response(
                HTTPStatus.OK,
                self.service.rankings(
                    metric=metric or "short_ratio",
                    limit=limit,
                    requested_date=requested_date,
                ),
            )
        if path == "/v1/signals/unusual":
            requested_date = str_arg(query, "date")
            min_total_volume = int_arg(query, "min_total_volume", 1_000_000)
            lookback = int_arg(query, "lookback", 10)
            return self.json_response(
                HTTPStatus.OK,
                self.service.unusual_signals(
                    requested_date=requested_date,
                    min_total_volume=min_total_volume,
                    lookback=lookback,
                ),
            )
        raise NotFoundError("Endpoint not found.")

    def handle_post(
        self, raw_path: str, body: bytes
    ) -> tuple[HTTPStatus, str, bytes, dict[str, str]]:
        parsed = urlsplit(raw_path)
        if parsed.path != "/v1/batch/lookup":
            raise NotFoundError("Endpoint not found.")

        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except json.JSONDecodeError as exc:
            raise BadRequestError("Request body must be valid JSON.") from exc
        return self.json_response(
            HTTPStatus.OK,
            self.service.batch_lookup(payload.get("symbols")),
        )


def make_handler(application: ShortPulseApplication):
    class ShortPulseHandler(BaseHTTPRequestHandler):
        server_version = "ShortPulseAPI/1.0"

        def do_OPTIONS(self) -> None:
            self.handle_with_app()

        def do_GET(self) -> None:
            self.handle_with_app()

        def do_POST(self) -> None:
            self.handle_with_app()

        def handle_with_app(self) -> None:
            content_length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(content_length) if content_length else b""
            status, content_type, response_body, extra_headers = application.handle(
                method=self.command,
                raw_path=self.path,
                body=body,
                headers=dict(self.headers),
            )
            self.send_response(status.value)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(response_body)))
            for name, value in extra_headers.items():
                self.send_header(name, value)
            self.end_headers()
            if response_body:
                self.wfile.write(response_body)

        def log_message(self, format: str, *args) -> None:
            return

    return ShortPulseHandler


def build_service_from_env() -> ShortPulseService:
    db_path = os.environ.get("SHORTPULSE_DB_PATH", "data/shortpulse.sqlite")
    fixture_dir = os.environ.get("SHORTPULSE_FIXTURE_DIR")
    today_value = os.environ.get("SHORTPULSE_TODAY")
    today = parse_iso_date(today_value) if today_value else None
    recent_ingest_ttl_seconds = env_int(
        "SHORTPULSE_RECENT_INGEST_TTL_SECONDS",
        DEFAULT_RECENT_INGEST_TTL_SECONDS,
    )
    recent_ingest_window_days = env_int(
        "SHORTPULSE_RECENT_INGEST_WINDOW_DAYS",
        DEFAULT_RECENT_INGEST_WINDOW_DAYS,
    )
    latest_probe_ttl_seconds = env_int(
        "SHORTPULSE_LATEST_PROBE_TTL_SECONDS",
        DEFAULT_LATEST_PROBE_TTL_SECONDS,
    )
    return ShortPulseService(
        db_path=db_path,
        fixture_dir=fixture_dir,
        today=today,
        recent_ingest_ttl_seconds=recent_ingest_ttl_seconds,
        recent_ingest_window_days=recent_ingest_window_days,
        latest_probe_ttl_seconds=latest_probe_ttl_seconds,
    )


def create_server(
    host: str = "0.0.0.0",
    port: int | None = None,
    service: ShortPulseService | None = None,
) -> ThreadingHTTPServer:
    application = ShortPulseApplication(service or build_service_from_env())
    resolved_port = port if port is not None else int(os.environ.get("PORT", "8000"))
    return ThreadingHTTPServer((host, resolved_port), make_handler(application))


def main() -> None:
    server = create_server()
    host, port = server.server_address
    print(f"ShortPulse API listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
