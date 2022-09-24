# Reconfiguring a running mysql instance:

```
mysql> set global binlog_format=ROW;
mysql> set global binlog_row_image=FULL;
```

note: `binlog_format` is a session-based property.  You will need to shutdown all active connections to fully convert
to row-based replication.


# GTID support
Maxwell contains support for
[GTID-based replication](https://dev.mysql.com/doc/refman/5.6/en/replication-gtids.html).
Enable it with the `--gtid_mode` configuration param.

Here's how you might configure your mysql server for GTID mode:

```
$ vi my.cnf

[mysqld]
server_id=1
log-bin=master
binlog_format=row
gtid-mode=ON
log-slave-updates=ON
enforce-gtid-consistency=true
```

When in GTID-mode, Maxwell will transparently pick up a new replication
position after a master change.  Note that you will still have to re-point
maxwell to the new master (or use a floating VIP)


# RDS 
To run Maxwell against RDS, (either Aurora or Mysql) you will need to do the following:

- set binlog_format to "ROW".  Do this in the "parameter groups" section.  For a Mysql-RDS instance this parameter will be
  in a "DB Parameter Group", for Aurora it will be in a "DB Cluster Parameter Group".
- setup RDS binlog retention as described [here](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.Concepts.MySQL.html).
  The tl;dr is to execute `call mysql.rds_set_configuration('binlog retention hours', 24)` on the server.

# Replicating and storing schema from different servers

Maxwell uses MySQL for 3 different functions:

1. A host to store the captured schema in (`--host`).
2. A host to replicate binlogs from (`--replication_host`).
3. A host to capture the schema from (`--schema_host`).

Often, all three hosts are the same.  `host` and `replication_host` should be different
if maxwell is chained off a replica.  `schema_host` should only be used when using the
maxscale replication proxy.

# Multiple Maxwells

Maxwell can operate with multiple instances running against a single master, in
different configurations.  This can be useful if you wish to have producers
running in different configurations, for example producing different groups of
tables to different topics.  Each instance of Maxwell must be configured with a
unique `client_id`, in order to store unique binlog positions.

With MySQL 5.5 and below, each replicator (be it mysql, maxwell, whatever) must
also be configured with a unique `replica_server_id`.  This is a 32-bit integer
that corresponds to mysql's `server_id` parameter.  The value you configure
should be unique across all mysql and maxwell instances.

# `--init_position`

This is a dangerous option that you really shouldn't use unless you know what
you're doing.  It allows you to "rewind" history and go back to a certain point
in the binlog.  This can work, but you should be aware that Maxwell must have
already "visited" that binlog position; simply specifying an arbitrary position
in the binlog will lead to Maxwell crashing. 


# Running with limited permissions

If the user you're running maxwell as can't view part of the database because of limited
permissions, Maxwell may be unable to capture information on part of the schem a and 
the replication stream can break with "Can't find table: XXX" errors.  In this case
you can enable the `ignore_missing_schema` flag *and* configure a filter that will exclude
any databases/tables you don't have permission to view. 

