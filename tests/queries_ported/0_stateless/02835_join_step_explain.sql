SET allow_experimental_analyzer = 1;

DROP STREAM IF EXISTS test_table_1;
CREATE STREAM test_table_1
(
    id uint64,
    value_1 string,
    value_2 uint64
) ENGINE=MergeTree ORDER BY id;

DROP STREAM IF EXISTS test_table_2;
CREATE STREAM test_table_2
(
    id uint64,
    value_1 string,
    value_2 uint64
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table_1(id, value_1, value_2) VALUES (0, 'Value', 0);
INSERT INTO test_table_2(id, value_1, value_2) VALUES (0, 'Value', 0);

EXPLAIN header = 1, actions = 1 SELECT lhs.id, lhs.value_1, rhs.id, rhs.value_1
FROM test_table_1 AS lhs INNER JOIN test_table_2 AS rhs ON lhs.id = rhs.id;

SELECT '--';

EXPLAIN header = 1, actions = 1 SELECT lhs.id, lhs.value_1, rhs.id, rhs.value_1
FROM test_table_1 AS lhs ASOF JOIN test_table_2 AS rhs ON lhs.id = rhs.id AND lhs.value_2 < rhs.value_2;

DROP STREAM test_table_1;
DROP STREAM test_table_2;
