-- Test that with default join_algorithm setting, we are doing a parallel hash join

SELECT value == 'hash,direct' FROM system.settings WHERE name = 'join_algorithm';
SET join_algorithm='direct,parallel_hash,hash';

EXPLAIN PIPELINE
SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 10
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 10
    ) t2
USING number
SETTINGS max_threads=16;

-- Test that join_algorithm = default does a hash join

SET join_algorithm='default';

SELECT value == 'default' FROM system.settings WHERE name = 'join_algorithm';

EXPLAIN PIPELINE
SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 10
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 10
    ) t2
USING number
SETTINGS max_threads=16;

SET join_algorithm=DEFAULT; -- reset

-- Check that compat setting also achieves a hash join

EXPLAIN PIPELINE
SELECT
    *
FROM
    (
        SELECT * FROM system.numbers LIMIT 10
    ) t1
    JOIN
    (
        SELECT * FROM system.numbers LIMIT 10
    ) t2
USING number
SETTINGS max_threads=16;
