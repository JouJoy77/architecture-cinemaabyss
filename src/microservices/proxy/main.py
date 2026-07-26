import logging
import os
import secrets
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import requests


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger('proxy-service')

HOP_BY_HOP_HEADERS = {
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailers',
    'transfer-encoding',
    'upgrade',
}


def env(name: str, default: str) -> str:
    return os.getenv(name, default).strip() or default


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def parse_percent(value: str) -> int:
    try:
        return min(100, max(0, int(value.strip())))
    except ValueError:
        return 0


class ProxyConfig:
    def __init__(self) -> None:
        self.port = int(env('PORT', '8000'))
        self.monolith_url = self.validate_url(env('MONOLITH_URL', 'http://localhost:8080'))
        self.movies_service_url = self.validate_url(env('MOVIES_SERVICE_URL', 'http://localhost:8081'))
        self.events_service_url = self.validate_url(env('EVENTS_SERVICE_URL', 'http://localhost:8082'))
        self.gradual_migration = parse_bool(env('GRADUAL_MIGRATION', 'false'))
        self.movies_migration_percent = parse_percent(env('MOVIES_MIGRATION_PERCENT', '0'))

    @staticmethod
    def validate_url(value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {'http', 'https'} or not parsed.netloc:
            raise ValueError(f'invalid upstream URL: {value!r}')
        return value.rstrip('/')

    def target_for(self, path: str) -> str:
        if path.startswith('/api/events'):
            return self.events_service_url
        if path.startswith('/api/movies/health'):
            return self.movies_service_url
        if path.startswith('/api/movies') and self.use_movies_service():
            return self.movies_service_url
        return self.monolith_url

    def use_movies_service(self) -> bool:
        if not self.gradual_migration or self.movies_migration_percent <= 0:
            return False
        if self.movies_migration_percent >= 100:
            return True
        return secrets.randbelow(100) < self.movies_migration_percent


class ProxyHandler(BaseHTTPRequestHandler):
    config: ProxyConfig

    def do_GET(self) -> None:
        if self.path == '/health':
            self.write_response(200, b'Strangler Fig Proxy is healthy', 'text/plain; charset=utf-8')
            return
        self.forward()

    def do_POST(self) -> None:
        self.forward()

    def do_PUT(self) -> None:
        self.forward()

    def do_PATCH(self) -> None:
        self.forward()

    def do_DELETE(self) -> None:
        self.forward()

    def do_OPTIONS(self) -> None:
        self.forward()

    def forward(self) -> None:
        started_at = time.monotonic()
        parsed_path = urlsplit(self.path)
        target = self.config.target_for(parsed_path.path)
        target_url = f'{target}{self.path}'
        content_length = int(self.headers.get('Content-Length', '0'))
        body = self.rfile.read(content_length) if content_length else None

        headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() not in HOP_BY_HOP_HEADERS | {'host', 'content-length'}
        }
        headers['X-Forwarded-Host'] = self.headers.get('Host', '')
        headers['X-Forwarded-Proto'] = self.headers.get('X-Forwarded-Proto', 'http')
        headers['X-CinemaAbyss-Upstream'] = urlsplit(target).netloc

        try:
            response = requests.request(
                method=self.command,
                url=target_url,
                headers=headers,
                data=body,
                timeout=30,
                allow_redirects=False,
            )
        except requests.RequestException as error:
            logger.error('proxy error for %s %s via %s: %s', self.command, self.path, target, error)
            self.write_response(502, b'upstream service is unavailable\n', 'text/plain; charset=utf-8')
            return

        self.send_response(response.status_code)
        for name, value in response.headers.items():
            if name.lower() not in HOP_BY_HOP_HEADERS | {'content-length'}:
                self.send_header(name, value)
        self.send_header('Content-Length', str(len(response.content)))
        self.end_headers()
        self.wfile.write(response.content)
        logger.info(
            '%s %s via %s completed in %dms',
            self.command,
            self.path,
            target,
            round((time.monotonic() - started_at) * 1000),
        )

    def write_response(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, message_format: str, *args: object) -> None:
        logger.info('%s - %s', self.address_string(), message_format % args)


def main() -> None:
    config = ProxyConfig()
    ProxyHandler.config = config
    server = ThreadingHTTPServer(('0.0.0.0', config.port), ProxyHandler)

    def shutdown(_signum: int, _frame: object) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    logger.info(
        'starting proxy on port %s, movies migration enabled=%s percent=%s',
        config.port,
        config.gradual_migration,
        config.movies_migration_percent,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
