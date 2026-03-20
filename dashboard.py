import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from config import load_config
from dashboard_data import fetch_dashboard_payload


INDEX_PATH = Path(__file__).with_name("dashboard_index.html")
STYLES_PATH = Path(__file__).with_name("dashboard_styles.css")
SCRIPT_PATH = Path(__file__).with_name("dashboard_app.js")


def main() -> None:
    config = load_config()
    default_db_path = config.sqlite_path if config.sqlite_path is not None else Path("immovision-local.db")

    parser = argparse.ArgumentParser(description="Dashboard local Immovision")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default=str(default_db_path))
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    handler_class = _build_handler(db_path)
    server = ThreadingHTTPServer((args.host, args.port), handler_class)
    print(f"Dashboard local: http://{args.host}:{args.port}")
    print(f"Base SQLite: {db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nArret du dashboard.")
    finally:
        server.server_close()


def _build_handler(db_path: Path):
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._serve_file(INDEX_PATH, "text/html; charset=utf-8")
                return
            if parsed.path == "/static/styles.css":
                self._serve_file(STYLES_PATH, "text/css; charset=utf-8")
                return
            if parsed.path == "/static/app.js":
                self._serve_file(SCRIPT_PATH, "application/javascript; charset=utf-8")
                return
            if parsed.path == "/api/dashboard":
                self._serve_dashboard_payload(db_path)
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Ressource introuvable")

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _serve_file(self, path: Path, content_type: str) -> None:
            if not path.exists():
                self.send_error(HTTPStatus.NOT_FOUND, "Fichier statique introuvable")
                return
            data = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _serve_dashboard_payload(self, db_path: Path) -> None:
            try:
                query = parse_qs(urlparse(self.path).query)
                live_only = _is_truthy_query_value(query.get("live_only"))
                payload = fetch_dashboard_payload(db_path, live_only=live_only)
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                status = HTTPStatus.OK
            except Exception as exc:
                body = json.dumps(
                    {"error": str(exc), "database_path": str(db_path)},
                    ensure_ascii=False,
                ).encode("utf-8")
                status = HTTPStatus.INTERNAL_SERVER_ERROR

            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return DashboardHandler


def _is_truthy_query_value(values: list[str] | None) -> bool:
    if not values:
        return False
    value = str(values[0]).strip().lower()
    return value in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    main()
