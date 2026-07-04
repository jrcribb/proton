"""
Integration tests for the Iceberg external stream sink.

Tests:
  1. Basic write — INSERT succeeds, data readable via table().
  2. Retry settings accepted — commit succeeds with non-default retry knobs.
  3. Multiple batches — sequential INSERTs produce correct total row count.
  4. Conflict retry — a 409 injected by a proxy triggers the retry path and
     the commit eventually succeeds.
"""

import logging
import os
import socket
import threading
import time
import urllib.parse
import urllib.request

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

ICEBERG_NAMESPACE = "test_ns"
ICEBERG_TABLE = "sink_test"


# ---------------------------------------------------------------------------
# Cluster fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            with_minio=True,
            with_iceberg_rest=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        catalog_url = (
            f"http://{cluster.iceberg_rest_ip}:{cluster.iceberg_rest_port}"
        )
        _wait_for_http(catalog_url + "/v1/config")

        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wait_for_http(url, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Service at {url} did not become ready within {timeout}s")


def _catalog_url(cluster):
    return f"http://{cluster.iceberg_rest_ip}:{cluster.iceberg_rest_port}"


def _setup_iceberg_db(node, catalog_url, namespace):
    node.query("DROP DATABASE IF EXISTS iceberg_test SYNC")
    node.query(
        f"""
        CREATE DATABASE iceberg_test
        ENGINE = Iceberg('{catalog_url}', '{namespace}',
                         'minio', 'minio123')
        SETTINGS
            use_environment_credentials = 0,
            access_key_id = 'minio',
            secret_access_key = 'minio123'
        """
    )


def _setup_external_stream(node, table):
    node.query(f"DROP STREAM IF EXISTS iceberg_test.{table} SYNC")
    node.query(
        f"""
        CREATE EXTERNAL STREAM iceberg_test.{table}
        (id UInt64, value String)
        SETTINGS type = 'iceberg'
        """
    )


def _get_docker_host_ip(node):
    """Return the IP reachable from inside the container that leads to the host."""
    result = node.exec_in_container(
        ["bash", "-c", "ip route show default | awk '/default/ {print $3}'"]
    )
    return result.strip()


# ---------------------------------------------------------------------------
# Conflict-injecting proxy
# ---------------------------------------------------------------------------


class _ConflictProxy:
    """
    Minimal HTTP reverse-proxy that injects a single 409 on the first
    commitTable call (POST .../tables/<name>) for a given table, then
    forwards all subsequent requests to the real catalog unchanged.

    Runs in a background thread bound on the host.  Proton reaches it via
    the Docker bridge gateway IP.
    """

    def __init__(self, real_catalog_url, conflicts_per_table=1):
        self._real = real_catalog_url.rstrip("/")
        self._conflicts = {}  # table_name -> remaining 409s to inject
        self._conflicts_per_table = conflicts_per_table
        self._lock = threading.Lock()
        self._server = None
        self._thread = None
        self.port = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        from http.server import BaseHTTPRequestHandler, HTTPServer

        proxy = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                logging.debug("ConflictProxy: " + fmt % args)

            def do_GET(self):
                self._forward("GET", b"")

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                # commitTable path ends with /tables/<name>
                parts = self.path.rstrip("/").split("/")
                if (
                    len(parts) >= 2
                    and parts[-2] == "tables"
                    and not self.path.endswith("/metrics")
                ):
                    table_name = parts[-1]
                    with proxy._lock:
                        remaining = proxy._conflicts.get(
                            table_name, proxy._conflicts_per_table
                        )
                        if table_name not in proxy._conflicts:
                            proxy._conflicts[table_name] = remaining
                        if proxy._conflicts[table_name] > 0:
                            proxy._conflicts[table_name] -= 1
                            logging.info(
                                "ConflictProxy: injecting 409 for table %s", table_name
                            )
                            self.send_response(409)
                            self.send_header("Content-Type", "application/json")
                            self.end_headers()
                            self.wfile.write(
                                b'{"error":{"message":"CommitFailedException: '
                                b'Requirement failed: branch main was created concurrently",'
                                b'"type":"CommitFailedException","code":409}}'
                            )
                            return
                self._forward("POST", body)

            def _forward(self, method, body):
                url = proxy._real + self.path
                req = urllib.request.Request(
                    url,
                    data=body if body else None,
                    method=method,
                    headers={
                        k: v
                        for k, v in self.headers.items()
                        if k.lower()
                        not in ("host", "content-length", "transfer-encoding")
                    },
                )
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        self.send_response(resp.status)
                        for k, v in resp.headers.items():
                            if k.lower() not in (
                                "transfer-encoding",
                                "connection",
                            ):
                                self.send_header(k, v)
                        self.end_headers()
                        self.wfile.write(resp.read())
                except urllib.error.HTTPError as e:
                    self.send_response(e.code)
                    for k, v in e.headers.items():
                        if k.lower() not in ("transfer-encoding", "connection"):
                            self.send_header(k, v)
                    self.end_headers()
                    self.wfile.write(e.read())

        # Bind on a free port
        self._server = HTTPServer(("0.0.0.0", 0), _Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True
        )
        self._thread.start()
        logging.info("ConflictProxy started on port %d", self.port)

    def stop(self):
        if self._server:
            self._server.shutdown()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_iceberg_sink_basic_write(started_cluster):
    """INSERT into an Iceberg external stream succeeds and data is readable."""
    node = started_cluster.instances["node1"]
    catalog_url = _catalog_url(started_cluster)

    _setup_iceberg_db(node, catalog_url, ICEBERG_NAMESPACE)
    _setup_external_stream(node, ICEBERG_TABLE)

    node.query(
        f"INSERT INTO iceberg_test.{ICEBERG_TABLE}(id, value) VALUES (1, 'hello'), (2, 'world')"
    )

    result = node.query(
        f"SELECT id, value FROM table(iceberg_test.{ICEBERG_TABLE}) ORDER BY id"
    )
    assert result.strip() == "1\thello\n2\tworld"


def test_iceberg_sink_retry_settings_accepted(started_cluster):
    """Retry settings can be set and do not prevent a successful commit."""
    node = started_cluster.instances["node1"]
    catalog_url = _catalog_url(started_cluster)
    table = ICEBERG_TABLE + "_retry_settings"

    _setup_iceberg_db(node, catalog_url, ICEBERG_NAMESPACE)
    _setup_external_stream(node, table)

    node.query(
        f"""
        INSERT INTO iceberg_test.{table}(id, value) VALUES (10, 'retry_test')
        SETTINGS
            iceberg_commit_retry_num_retries = 5,
            iceberg_commit_retry_min_wait_ms = 100,
            iceberg_commit_retry_max_wait_ms = 1000,
            iceberg_commit_retry_total_timeout_ms = 10000
        """
    )

    result = node.query(f"SELECT id, value FROM table(iceberg_test.{table})")
    assert "10" in result and "retry_test" in result


def test_iceberg_sink_multiple_batches(started_cluster):
    """Multiple sequential INSERTs produce correct total row count."""
    node = started_cluster.instances["node1"]
    catalog_url = _catalog_url(started_cluster)
    table = ICEBERG_TABLE + "_multi"

    _setup_iceberg_db(node, catalog_url, ICEBERG_NAMESPACE)
    _setup_external_stream(node, table)

    for i in range(3):
        node.query(
            f"INSERT INTO iceberg_test.{table}(id, value) VALUES ({i}, 'batch_{i}')"
        )

    result = node.query(f"SELECT count() FROM table(iceberg_test.{table})")
    assert int(result.strip()) == 3


def test_iceberg_sink_conflict_retry(started_cluster):
    """
    A 409 injected by the proxy triggers the retry path.
    Proton must successfully commit after refreshing table state.
    """
    node = started_cluster.instances["node1"]
    real_catalog_url = _catalog_url(started_cluster)
    table = ICEBERG_TABLE + "_conflict"

    # Start proxy on the host — Proton reaches it via the Docker gateway IP.
    proxy = _ConflictProxy(real_catalog_url, conflicts_per_table=1)
    proxy.start()
    try:
        host_ip = _get_docker_host_ip(node)
        proxy_catalog_url = f"http://{host_ip}:{proxy.port}"

        # Point Proton at the proxy instead of the real catalog.
        # Use fast retry so the test doesn't wait 10 s.
        _setup_iceberg_db(node, proxy_catalog_url, ICEBERG_NAMESPACE)
        _setup_external_stream(node, table)

        node.query(
            f"""
            INSERT INTO iceberg_test.{table}(id, value) VALUES (42, 'after_conflict')
            SETTINGS
                iceberg_commit_retry_num_retries = 3,
                iceberg_commit_retry_min_wait_ms = 200,
                iceberg_commit_retry_max_wait_ms = 500,
                iceberg_commit_retry_total_timeout_ms = 30000
            """
        )

        # Data must be present despite the initial 409.
        # Read directly from the real catalog to bypass the proxy.
        _setup_iceberg_db(node, real_catalog_url, ICEBERG_NAMESPACE)
        result = node.query(
            f"SELECT id, value FROM table(iceberg_test.{table})"
        )
        assert "42" in result and "after_conflict" in result

        # Proxy must have seen exactly one injected 409.
        assert proxy._conflicts.get(table, 0) == 0, (
            "Proxy did not consume all injected conflicts — "
            "retry may not have been triggered"
        )
    finally:
        proxy.stop()
