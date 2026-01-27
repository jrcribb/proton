drop view if exists 99079_mv;
drop view if exists 99079_mv2;
drop stream if exists 99079_stream;
drop stream if exists 99079_append;
drop stream if exists 99079_vk;
drop stream if exists 99079_kv;

create stream 99079_stream(i int);
create stream 99079_append(i int, v string) settings flush_threshold_count=1;
create stream 99079_vk(i int, v string) primary key i settings mode='versioned_kv', flush_threshold_count=1;
create stream 99079_kv(i int, v string) primary key i settings mode='versioned_kv', flush_threshold_count=1;

--- Comma Join Stream, VersionedKV, VersionedKV Stream
create materialized view 99079_mv as select 99079_stream.i as i, append.v as t, vk.v as v, kv.v as x, block_size() as size from 99079_stream, 99079_append as append, 99079_vk as vk, 99079_kv as kv settings max_joined_block_size_rows=4;
--- Cross Join Stream, VersionedKV, VersionedKV Stream
create materialized view 99079_mv2 as select 99079_stream.i as i, append.v as t, vk.v as v, kv.v as x, block_size() as size from 99079_stream cross join 99079_append as append cross join 99079_vk as vk cross join 99079_kv as kv settings max_joined_block_size_rows=4;

select sleep(2) format Null;
insert into 99079_append(i, v) values(1, 't1');
insert into 99079_vk(i, v) values(1, 'v1');
insert into 99079_kv(i, v) values(1, 'x1');
select sleep(2) format Null;
insert into 99079_stream(i) values(1);

insert into 99079_append(i, v) values(2, 't2');
insert into 99079_vk(i, v) values(2, 'v2');
insert into 99079_kv(i, v) values(2, 'x2');
select sleep(2) format Null;
insert into 99079_stream(i) values(2);

select sleep(3) format Null;
select * except _tp_time from table(99079_mv) order by i, t, v, x;
select '================';
select * except _tp_time from table(99079_mv2) order by i, t, v, x;

drop view if exists 99079_mv;
drop view if exists 99079_mv2;
drop stream if exists 99079_stream;
drop stream if exists 99079_append;
drop stream if exists 99079_vk;
drop stream if exists 99079_kv;
