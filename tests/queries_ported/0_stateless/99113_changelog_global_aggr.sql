DROP VIEW IF EXISTS 99113_mv;
DROP VIEW IF EXISTS 99113_mv2;
DROP VIEW IF EXISTS 99113_mv3;
DROP VIEW IF EXISTS 99113_mv4;
DROP STREAM IF EXISTS 99113_kv;

--- Single shard
CREATE STREAM 99113_kv
(
    `key` string,
    `value` int
)
PRIMARY KEY key
settings mode='versioned_kv';

create materialized view 99113_mv as select key, sum(value) as max_v from 99113_kv group by key emit on update settings default_hash_table='memory' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99113_mv2 as select sum(value) as max_v from 99113_kv emit on update settings default_hash_table='memory' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99113_mv3 as select key, sum(value) as max_v from 99113_kv group by key emit on update settings default_hash_table='hybrid' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99113_mv4 as select sum(value) as max_v from 99113_kv emit on update settings default_hash_table='hybrid' STORAGE_SETTINGS flush_threshold_count=1;

select sleep(2) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k1', 10);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k2', 20);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k3', 30);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k4', 40);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k3', 300);

select sleep(3) FORMAT Null;
select '(Single shard)';
select '=== memory aggr with group by key ===';
select * except _tp_time from table(99113_mv) order by _tp_sn;
select '=== memory aggr without group by key ===';
select * except _tp_time from table(99113_mv2) order by _tp_sn;
select '=== hybrid aggr with group by key ===';
select * except _tp_time from table(99113_mv3) order by _tp_sn;
select '=== hybrid aggr without group by key ===';
select * except _tp_time from table(99113_mv4) order by _tp_sn;

--- Multi shards
DROP VIEW IF EXISTS 99113_mv;
DROP VIEW IF EXISTS 99113_mv2;
DROP VIEW IF EXISTS 99113_mv3;
DROP VIEW IF EXISTS 99113_mv4;
DROP STREAM IF EXISTS 99113_kv;

CREATE STREAM 99113_kv
(
    `key` string,
    `value` int
)
PRIMARY KEY key
SETTINGS shards = 3, mode='versioned_kv';

create materialized view 99113_mv as select key, sum(value) as max_v from 99113_kv group by key emit on update settings default_hash_table='memory' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99113_mv2 as select sum(value) as max_v from 99113_kv emit on update settings default_hash_table='memory' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99113_mv3 as select key, sum(value) as max_v from 99113_kv group by key emit on update settings default_hash_table='hybrid' STORAGE_SETTINGS flush_threshold_count=1;
create materialized view 99113_mv4 as select sum(value) as max_v from 99113_kv emit on update settings default_hash_table='hybrid' STORAGE_SETTINGS flush_threshold_count=1;

select sleep(2) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k1', 10);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k2', 20);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k3', 30);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k4', 40);
select sleep(1) FORMAT Null;
INSERT INTO 99113_kv(key, value) values('k3', 300);

select sleep(3) FORMAT Null;
select '-------------------------------------------------';
select '(Multi shards)';
select '=== memory aggr with group by key ===';
select * except _tp_time from table(99113_mv) order by _tp_sn;
select '=== memory aggr without group by key ===';
select * except _tp_time from table(99113_mv2) order by _tp_sn;
select '=== hybrid aggr with group by key ===';
select * except _tp_time from table(99113_mv3) order by _tp_sn;
select '=== hybrid aggr without group by key ===';
select * except _tp_time from table(99113_mv4) order by _tp_sn;

DROP VIEW IF EXISTS 99113_mv;
DROP VIEW IF EXISTS 99113_mv2;
DROP VIEW IF EXISTS 99113_mv3;
DROP VIEW IF EXISTS 99113_mv4;
DROP STREAM IF EXISTS 99113_kv;
