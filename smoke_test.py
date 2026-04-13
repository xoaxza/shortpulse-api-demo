from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path
from unittest.mock import patch

from app import ShortPulseApplication, create_server
from shortpulse import ShortPulseService, UpstreamError


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "finra"


class FakeResponse:
    def __init__(self, body: str) -> None:
        self.body = body.encode("utf-8")

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return self.body


class SmokeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.service = ShortPulseService(
            db_path=Path(cls.tmpdir.name) / "shortpulse.sqlite",
            fixture_dir=FIXTURE_DIR,
        )
        try:
            cls.server = create_server(host="127.0.0.1", port=0, service=cls.service)
        except PermissionError as exc:
            cls.tmpdir.cleanup()
            raise unittest.SkipTest(f"localhost TCP sockets unavailable: {exc}") from exc
        host, port = cls.server.server_address
        cls.base_url = f"http://{host}:{port}"
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.wait_for_server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=5)
        cls.tmpdir.cleanup()

    @classmethod
    def wait_for_server(cls) -> None:
        last_error: OSError | None = None
        for _ in range(20):
            try:
                with urllib.request.urlopen(f"{cls.base_url}/", timeout=1) as response:
                    response.read()
                return
            except OSError as exc:
                last_error = exc
                time.sleep(0.05)
        if last_error:
            raise last_error

    def request(
        self, method: str, path: str, payload: dict | None = None
    ) -> tuple[int, str, bytes]:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            method=method,
            data=data,
            headers=headers,
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.headers.get("Content-Type", ""), response.read()

    def fetch_json(self, path: str) -> dict:
        status, content_type, body = self.request("GET", path)
        self.assertEqual(status, 200, body.decode("utf-8"))
        self.assertIn("application/json", content_type)
        return json.loads(body.decode("utf-8"))

    def fetch_html(self, path: str) -> str:
        status, content_type, body = self.request("GET", path)
        self.assertEqual(status, 200, body.decode("utf-8"))
        self.assertIn("text/html", content_type)
        return body.decode("utf-8")

    def post_json(self, path: str, payload: dict) -> dict:
        status, content_type, body = self.request("POST", path, payload=payload)
        self.assertEqual(status, 200, body.decode("utf-8"))
        self.assertIn("application/json", content_type)
        return json.loads(body.decode("utf-8"))

    def test_smoke_endpoints(self) -> None:
        html = self.fetch_html("/")
        self.assertIn("ShortPulse API", html)

        status_payload = self.fetch_json("/v1/status")
        self.assertTrue(status_payload["ok"])
        self.assertIn("not short interest", status_payload["disclaimer"].lower())
        self.assertEqual(status_payload["data"]["latest_trade_date"], "2024-04-12")
        self.assertEqual(status_payload["data"]["latest_row_count"], 5)

        latest_payload = self.fetch_json("/v1/ticker/GME/latest")
        self.assertEqual(latest_payload["data"]["symbol"], "GME")
        self.assertEqual(latest_payload["data"]["trade_date"], "2024-04-12")
        self.assertGreater(latest_payload["data"]["short_ratio"], 0.8)

        history_payload = self.fetch_json("/v1/ticker/AAPL/history?days=3")
        self.assertEqual(history_payload["data"]["returned_days"], 3)
        self.assertEqual(
            [item["trade_date"] for item in history_payload["data"]["history"]],
            ["2024-04-10", "2024-04-11", "2024-04-12"],
        )

        rankings_payload = self.fetch_json("/v1/rankings?metric=short_ratio&limit=3")
        self.assertEqual(rankings_payload["data"]["results"][0]["symbol"], "GME")

        signals_payload = self.fetch_json(
            "/v1/signals/unusual?lookback=4&min_total_volume=1000000"
        )
        self.assertEqual(signals_payload["data"]["results"][0]["symbol"], "GME")
        self.assertGreater(signals_payload["data"]["results"][0]["volume_spike"], 6.0)

        batch_payload = self.post_json(
            "/v1/batch/lookup",
            {"symbols": ["AAPL", "GME", "XXXX"]},
        )
        self.assertEqual(
            [item["symbol"] for item in batch_payload["data"]["found"]],
            ["AAPL", "GME"],
        )
        self.assertEqual(batch_payload["data"]["missing_symbols"], ["XXXX"])


class ProbeBehaviorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.service = ShortPulseService(
            db_path=Path(self.tmpdir.name) / "shortpulse.sqlite",
            today=date(2024, 4, 14),
        )
        self.fixture_text = (FIXTURE_DIR / "CNMSshvol20240412.txt").read_text(
            encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def fake_urlopen(self, request: urllib.request.Request, timeout: int = 20) -> FakeResponse:
        url = request.full_url
        if url.endswith("20240414.txt") or url.endswith("20240413.txt"):
            raise urllib.error.HTTPError(url, 403, "Forbidden", hdrs=None, fp=None)
        if url.endswith("20240412.txt"):
            return FakeResponse(self.fixture_text)
        raise AssertionError(f"Unexpected URL requested in probe test: {url}")

    def test_latest_probe_treats_403_as_missing(self) -> None:
        with patch("urllib.request.urlopen", side_effect=self.fake_urlopen):
            self.assertEqual(self.service.latest_available_trade_date(), "2024-04-12")

    def test_explicit_date_still_surfaces_403(self) -> None:
        with patch("urllib.request.urlopen", side_effect=self.fake_urlopen):
            with self.assertRaises(UpstreamError):
                self.service.require_trade_date("2024-04-13")


class SymbolHandlingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.service = ShortPulseService(
            db_path=Path(self.tmpdir.name) / "shortpulse.sqlite",
            fixture_dir=FIXTURE_DIR,
        )
        self.app = ShortPulseApplication(self.service)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_parse_rows_keeps_slash_symbols_and_skips_invalid_rows(self) -> None:
        rows = self.service.parse_rows(
            "\n".join(
                [
                    "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market",
                    "20240412|BRK/B|100|0|200|NMS",
                    "20240412|BAD$SYM|50|0|100|NMS",
                    "20240412|AAPL|75|0|150|NMS",
                ]
            ),
            "fixture://symbol-handling",
        )

        self.assertEqual([row["symbol"] for row in rows], ["BRK/B", "AAPL"])

    def test_parse_rows_accepts_fractional_finra_volumes(self) -> None:
        rows = self.service.parse_rows(
            "\n".join(
                [
                    "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market",
                    "20240412|TSLA|170064.090975|42.5|340128.18195|NMS",
                    "20240412|AAPL|170064|0|340128|NMS",
                ]
            ),
            "fixture://fractional-volumes",
        )

        self.assertEqual(len(rows), 2)
        self.assertIsInstance(rows[0]["short_volume"], float)
        self.assertIsInstance(rows[0]["short_exempt_volume"], float)
        self.assertIsInstance(rows[0]["total_volume"], float)
        self.assertEqual(rows[0]["short_volume"], 170064.090975)
        self.assertEqual(rows[0]["short_exempt_volume"], 42.5)
        self.assertEqual(rows[0]["total_volume"], 340128.18195)
        self.assertEqual(rows[0]["short_ratio"], 0.5)
        self.assertIsInstance(rows[1]["short_volume"], int)
        self.assertIsInstance(rows[1]["short_exempt_volume"], int)
        self.assertIsInstance(rows[1]["total_volume"], int)
        self.assertEqual(rows[1]["short_ratio"], 0.5)

    def test_ticker_endpoint_decodes_url_encoded_symbol_segments(self) -> None:
        with self.service._connect() as conn:
            conn.execute(
                """
                INSERT INTO daily_short_volume (
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
                (
                    "2024-04-12",
                    "BRK/B",
                    100,
                    0,
                    200,
                    "NMS",
                    0.5,
                    "fixture://symbol-handling",
                    "2024-04-12T00:00:00Z",
                ),
            )

        status, content_type, body, _ = self.app.handle(
            "GET", "/v1/ticker/BRK%2FB/latest"
        )

        self.assertEqual(status, 200)
        self.assertIn("application/json", content_type)
        payload = json.loads(body.decode("utf-8"))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"]["symbol"], "BRK/B")


if __name__ == "__main__":
    unittest.main()
