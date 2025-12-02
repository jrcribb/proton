-- Tags: use-rocksdb
SET query_mode='table';

DROP STREAM IF EXISTS rdb;
DROP STREAM IF EXISTS t2;

CREATE MUTABLE STREAM rdb ( `key` uint32, `value` string )
PRIMARY KEY key;
select sleep(2) format Null;
INSERT INTO rdb(key, value) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

CREATE STREAM t2 ( `k` uint16 );
select sleep(2) format Null;
INSERT INTO t2(k) VALUES (4), (5), (6);

SELECT value == 'direct,hash' FROM system.settings WHERE name = 'join_algorithm';

select sleep(2) format Null;
SET join_algorithm = 'direct, parallel_hash, hash';

SELECT count_if(explain like '%Algorithm: DirectKeyValueJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT * FROM ( SELECT k AS key FROM t2 ) AS t2
    INNER JOIN rdb ON rdb.key = t2.key
    ORDER BY key ASC
);

SET join_algorithm = 'direct, hash';

SELECT value == 'direct,hash' FROM system.settings WHERE name = 'join_algorithm';

SELECT count_if(explain like '%Algorithm: DirectKeyValueJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT * FROM ( SELECT k AS key FROM t2 ) AS t2
    INNER JOIN rdb ON rdb.key = t2.key
    ORDER BY key ASC
);

SET join_algorithm = 'hash, direct';

SELECT value == 'hash,direct' FROM system.settings WHERE name = 'join_algorithm';

SELECT count_if(explain like '%Algorithm: DirectKeyValueJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT * FROM ( SELECT k AS key FROM t2 ) AS t2
    INNER JOIN rdb ON rdb.key = t2.key
    ORDER BY key ASC
);

SET join_algorithm = 'grace_hash,hash';

SELECT value == 'grace_hash,hash' FROM system.settings WHERE name = 'join_algorithm';

SELECT count_if(explain like '%Algorithm: GraceHashJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT * FROM ( SELECT number AS key, number * 10 AS key2 FROM numbers_mt(10) ) AS t1
    JOIN ( SELECT k AS key, k + 100 AS key2 FROM t2 ) AS t2 ON t1.key = t2.key OR t1.key2 = t2.key2
);

SELECT count_if(explain like '%Algorithm: GraceHashJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT * FROM ( SELECT number AS key, number * 10 AS key2 FROM numbers_mt(10) ) AS t1
    JOIN ( SELECT k AS key, k + 100 AS key2 FROM t2 ) AS t2 ON t1.key = t2.key
);

SET join_algorithm = 'grace_hash, hash, auto';

SELECT value = 'grace_hash,hash,auto' FROM system.settings WHERE name = 'join_algorithm';


DROP DICTIONARY IF EXISTS dict;
DROP STREAM IF EXISTS src;

CREATE STREAM src (id uint64, s string) ORDER BY id;
select sleep(2) format Null;;
INSERT INTO src(id, s) SELECT number, to_string(number) FROM numbers(1000000);

select sleep(2) format Null;
CREATE DICTIONARY dict(
  id uint64,
  s  string
) PRIMARY KEY id
SOURCE(TIMEPLUS(STREAM 'src' DB current_database()))
LIFETIME (MIN 0 MAX 0)
LAYOUT(HASHED());

SET join_algorithm = 'default';

SELECT count_if(explain like '%Algorithm: DirectKeyValueJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN actions = 1
    SELECT s FROM (SELECT to_uint64(9911) id) t1 INNER JOIN dict t2 USING (id)
);

SET join_algorithm = 'direct,hash';
SELECT count_if(explain like '%Algorithm: DirectKeyValueJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN actions = 1
    SELECT s FROM (SELECT to_uint64(9911) id) t1 INNER JOIN dict t2 USING (id)
);

SET join_algorithm = 'hash,direct';
SELECT count_if(explain like '%Algorithm: DirectKeyValueJoin%'), count_if(explain like '%Algorithm: HashJoin%') FROM (
    EXPLAIN actions = 1
    SELECT s FROM (SELECT to_uint64(9911) id) t1 INNER JOIN dict t2 USING (id)
);
