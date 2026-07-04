import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

ICEBERG_NAMESPACE = "test_ns"
ICEBERG_TABLE = "sink_test"


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

        # Wait for the REST catalog to be ready before running tests.
        catalog_url = f"http://{cluster.iceberg_rest_ip}:{cluster.iceberg_rest_port}"
        _wait_for_catalog(catalog_url)

        yield cluster
    finally:
        cluster.shutdown()


def _wait_for_catalog(url, timeout=60):
    import urllib.request

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{url}/v1/config", timeout=2)
            logging.info("Iceberg REST catalog is up at %s", url)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"Iceberg REST catalog did not start within {timeout}s")


def _catalog_url(cluster):
    return f"http://{cluster.iceberg_rest_ip}:{cluster.iceberg_rest_port}"


def _setup_iceberg_db(node, catalog_url, namespace):
    node.query(f"DROP DATABASE IF EXISTS iceberg_test SYNC")
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

    _setup_iceberg_db(node, catalog_url, ICEBERG_NAMESPACE)
    _setup_external_stream(node, ICEBERG_TABLE + "_retry")

    node.query(
        f"""
        INSERT INTO iceberg_test.{ICEBERG_TABLE}_retry(id, value) VALUES (10, 'retry_test')
        SETTINGS
            iceberg_commit_retry_num_retries = 5,
            iceberg_commit_retry_min_wait_ms = 100,
            iceberg_commit_retry_max_wait_ms = 1000,
            iceberg_commit_retry_total_timeout_ms = 10000
        """
    )

    result = node.query(
        f"SELECT id, value FROM table(iceberg_test.{ICEBERG_TABLE}_retry)"
    )
    assert "10" in result and "retry_test" in result


def test_iceberg_sink_multiple_batches(started_cluster):
    """Multiple sequential INSERTs produce distinct snapshots, all data readable."""
    node = started_cluster.instances["node1"]
    catalog_url = _catalog_url(started_cluster)

    table = ICEBERG_TABLE + "_multi"
    _setup_iceberg_db(node, catalog_url, ICEBERG_NAMESPACE)
    _setup_external_stream(node, table)

    for i in range(3):
        node.query(
            f"INSERT INTO iceberg_test.{table}(id, value) VALUES ({i}, 'batch_{i}')"
        )

    result = node.query(
        f"SELECT count() FROM table(iceberg_test.{table})"
    )
    assert int(result.strip()) == 3
