create table t1 (t1 timestamp not null default now(), t2 datetime, t3 timestamp NOT NULL DEFAULT '0000-00-00 00:00:00')
create table t1 (t1 timestamp not null default now() on update now(), t2 datetime)
REPAIR TABLE federated.t1
REPAIR TABLE federated.t1 QUICK
REPAIR TABLE federated.t1 EXTENDED
REPAIR TABLE federated.t1 USE_FRM
CREATE TABLE test_long_data(col1 int, col2 long varchar, col3 long varbinary)
