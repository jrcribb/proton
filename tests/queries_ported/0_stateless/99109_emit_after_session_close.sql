drop view if exists 99109_mv;
drop view if exists 99109_only_mv;
drop view if exists 99109_with_session_start_col_mv;
drop view if exists 99109_with_session_end_col_mv;
drop view if exists 99109_with_session_start_and_end_col_mv;
drop view if exists 99109_merge_open_sessions;
drop view if exists 99109_merge_exclude_session_end;
drop view if exists 99109_consecutive_fails;

CREATE STREAM IF NOT EXISTS devices(
    `Device` string,
    `Type` string, -- 'assoc' -> 'auth' -> 'dhcp' -> 'dns' -> 'connection'
    `Subtype` string -- 'success', 'failed'
);

CREATE MATERIALIZED VIEW 99109_mv
AS
WITH connect_stage_events AS
(
    SELECT *, Type = 'assoc' AS session_start, Type = 'connection' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       count()                      AS events,
       count_if(Subtype = 'failed') AS fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device
EMIT AFTER SESSION CLOSE WITH MAXSPAN 1s AND TIMEOUT 2s
SETTINGS default_hash_table='hybrid';

CREATE MATERIALIZED VIEW 99109_only_mv
AS
WITH connect_stage_events AS
(
    SELECT *, Type = 'assoc' AS session_start, Type = 'connection' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       count()                      AS events,
       count_if(Subtype = 'failed') AS fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device
EMIT AFTER SESSION CLOSE WITH ONLY MAXSPAN 1s AND TIMEOUT 2s
SETTINGS default_hash_table='hybrid';

CREATE MATERIALIZED VIEW 99109_with_session_start_col_mv
AS
WITH connect_stage_events AS
(
    SELECT *, Type = 'assoc' AS session_start, Type = 'connection' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       count()                      AS events,
       count_if(Subtype = 'failed') AS fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device
EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, session_start, false) WITH MAXSPAN 1s AND TIMEOUT 2s
SETTINGS default_hash_table='hybrid';

CREATE MATERIALIZED VIEW 99109_with_session_end_col_mv
AS
WITH connect_stage_events AS
(
    SELECT *, Type = 'assoc' AS session_start, Type = 'connection' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       count()                      AS events,
       count_if(Subtype = 'failed') AS fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device
EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, true, session_end) WITH MAXSPAN 1s AND TIMEOUT 2s
SETTINGS default_hash_table='hybrid';

CREATE MATERIALIZED VIEW 99109_with_session_start_and_end_col_mv
AS
WITH connect_stage_events AS
(
    SELECT *, Type = 'assoc' AS session_start, Type = 'connection' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       count()                      AS events,
       count_if(Subtype = 'failed') AS fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device
EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, session_start, session_end) WITH MAXSPAN 1s AND TIMEOUT 2s
SETTINGS default_hash_table='hybrid';

CREATE MATERIALIZED VIEW 99109_merge_open_sessions
AS
WITH connect_stage_events AS
(
    SELECT *, Type = 'assoc' AS session_start, Type = 'connection' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       count()                      AS events,
       count_if(Subtype = 'failed') AS fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device
EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, session_start, session_end) WITH MAXSPAN 1s AND TIMEOUT 2s
SETTINGS merge_open_sessions = true, default_hash_table='hybrid';

CREATE MATERIALIZED VIEW 99109_merge_exclude_session_end
AS
WITH connect_stage_events AS
(
    SELECT *, Type = 'assoc' AS session_start, Type = 'connection' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       count()                      AS events,
       count_if(Subtype = 'failed') AS fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device
EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, session_start, session_end) WITH MAXSPAN 1s AND TIMEOUT 2s
SETTINGS include_session_end = false, default_hash_table='hybrid';

CREATE MATERIALIZED VIEW 99109_consecutive_fails
AS
WITH connect_stage_events AS
(
    SELECT *,  Subtype = 'failed' AS session_start, Subtype = 'success' AS session_end FROM devices WHERE Type IN ('assoc', 'auth', 'dhcp', 'dns', 'connection')
)
SELECT Device,
       Type,
       count()                      AS consecutive_fails,
       min(_tp_time)                AS session_start_ts,
       max(_tp_time)                AS session_end_ts
FROM connect_stage_events
GROUP BY Device, Type
EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, session_start, session_end) WITH MAXSPAN 1s AND TIMEOUT 2s
SETTINGS include_session_end = false, merge_open_sessions=true, default_hash_table='hybrid';

SELECT sleep(2) format Null;

-- TIMEOUT or session_end trigger close
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.000');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'auth', 'success', '2025-01-01 00:00:00.001');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dhcp', 'success', '2025-01-01 00:00:00.002');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dns', 'success', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'connection', 'success', '2025-01-01 00:00:00.004');

SELECT sleep(2) format Null;
SELECT sleep(2) format Null; -- EMIT ... TIMEOUT will happen for some MV

-- MAXSPAN or session_end trigger close
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.000');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'auth', 'success', '2025-01-01 00:00:00.001');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dhcp', 'success', '2025-01-01 00:00:00.002');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dns', 'success', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'connection', 'success', '2025-01-01 00:00:01.100');

-- MAXSPAN or session_end trigger close (out of order)
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'auth', 'success', '2025-01-01 00:00:00.001');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.000');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dhcp', 'success', '2025-01-01 00:00:00.002');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dns', 'success', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'connection', 'success', '2025-01-01 00:00:01.100');

-- MAXSPAN or session_end trigger close, multi-open coz dup events
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.000');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'auth', 'success', '2025-01-01 00:00:00.001');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.000');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dhcp', 'success', '2025-01-01 00:00:00.002');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dns', 'success', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'connection', 'success', '2025-01-01 00:00:01.100');

SELECT sleep(2) format Null;

-- FAILS
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'failed', '2025-01-01 00:00:00.000');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'failed', '2025-01-01 00:00:00.001');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'failed', '2025-01-01 00:00:00.002');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'auth', 'success', '2025-01-01 00:00:00.001');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dhcp', 'success', '2025-01-01 00:00:00.002');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dns', 'success', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'connection', 'success', '2025-01-01 00:00:01.100');

INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'auth', 'success', '2025-01-01 00:00:00.001');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'failed', '2025-01-01 00:00:00.002');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'failed', '2025-01-01 00:00:00.003');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'assoc', 'success', '2025-01-01 00:00:00.004');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dhcp', 'success', '2025-01-01 00:00:00.005');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'dns', 'success', '2025-01-01 00:00:00.006');
INSERT INTO devices (Device, Type, Subtype, _tp_time) VALUES ('dev1', 'connection', 'success', '2025-01-01 00:00:01.100');

SELECT sleep(2) format Null;

SELECT '-------99109_mv---------';
SELECT Device, events, fails, session_start_ts, session_end_ts FROM table(99109_mv) ORDER BY _tp_time ASC;
SELECT '-------99109_only_mv---------';
SELECT Device, events, fails, session_start_ts, session_end_ts FROM table(99109_only_mv) ORDER BY _tp_time ASC;
SELECT '-------99109_with_session_start_col_mv---------';
SELECT Device, events, fails, session_start_ts, session_end_ts FROM table(99109_with_session_start_col_mv) ORDER BY _tp_time ASC;
SELECT '-------99109_with_session_end_col_mv---------';
SELECT Device, events, fails, session_start_ts, session_end_ts FROM table(99109_with_session_end_col_mv) ORDER BY _tp_time ASC;
SELECT '-------99109_with_session_start_and_end_col_mv---------';
SELECT Device, events, fails, session_start_ts, session_end_ts FROM table(99109_with_session_start_and_end_col_mv) ORDER BY _tp_time ASC;
SELECT '-------99109_merge_open_sessions---------';
SELECT Device, events, fails, session_start_ts, session_end_ts FROM table(99109_merge_open_sessions) ORDER BY _tp_time ASC;
SELECT '-------99109_merge_exclude_session_end---------';
SELECT Device, events, fails, session_start_ts, session_end_ts FROM table(99109_merge_exclude_session_end) ORDER BY _tp_time ASC;
SELECT '-------99109_consecutive_fails---------';
SELECT Device, Type, consecutive_fails, session_start_ts, session_end_ts FROM table(99109_consecutive_fails) ORDER BY _tp_time ASC;

drop view if exists 99109_mv;
drop view if exists 99109_only_mv;
drop view if exists 99109_with_session_start_col_mv;
drop view if exists 99109_with_session_end_col_mv;
drop view if exists 99109_with_session_start_and_end_col_mv;
drop view if exists 99109_merge_open_sessions;
drop view if exists 99109_merge_exclude_session_end;
drop view if exists 99109_consecutive_fails;
