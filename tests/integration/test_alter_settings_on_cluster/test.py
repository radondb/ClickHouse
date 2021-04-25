import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "configs/config.d/clusters1.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "configs/config.d/clusters2.xml",
        "configs/config.d/distributed_ddl.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        ch2.query("CREATE DATABASE test_default_database;")
        yield cluster

    finally:
        cluster.shutdown()


def test_default_database_on_cluster(started_cluster):
    ch1.query(
        database="test_default_database",
        sql="CREATE TABLE test_local_table (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/test_local_table', '{replica}') ORDER BY tuple();",
    )

    ch2.query(
        database="test_default_database",
        sql="ALTER TABLE test_local_table MODIFY SETTING old_parts_lifetime = 100;",
    )

    for node in [ch1, ch2]:
        assert node.query(
            database="test_default_database",
            sql="SHOW CREATE test_local_table FORMAT TSV",
        ).endswith("old_parts_lifetime = 100\n")
