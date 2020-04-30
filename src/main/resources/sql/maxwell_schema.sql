CREATE TABLE IF NOT EXISTS `schemas` (
  id int unsigned auto_increment NOT NULL primary key,
  binlog_file varchar(255),
  binlog_position int unsigned,
  last_heartbeat_read bigint null default 0,
  gtid_set varchar(4096),
  base_schema_id int unsigned NULL default NULL,
  deltas mediumtext charset 'utf8' NULL default NULL,
  server_id int unsigned,
  position_sha char(40) CHARACTER SET latin1 DEFAULT NULL,
  charset varchar(255),
  version smallint unsigned not null default 0,
  deleted tinyint(1) not null default 0,
  UNIQUE KEY `position_sha` (`position_sha`)
);

CREATE TABLE IF NOT EXISTS `databases` (
  id        int unsigned auto_increment NOT NULL primary key,
  schema_id int unsigned,
  name      varchar(255) charset 'utf8',
  charset   varchar(255),
  index (schema_id)
);

CREATE TABLE IF NOT EXISTS `tables` (
  id          int unsigned auto_increment NOT NULL primary key,
  schema_id   int unsigned,
  database_id int unsigned,
  name      varchar(255) charset 'utf8',
  charset   varchar(255),
  pk        varchar(1024) charset 'utf8',
  index (schema_id),
  index (database_id)
);

CREATE TABLE IF NOT EXISTS `columns` (
  id          int unsigned auto_increment NOT NULL primary key,
  schema_id   int unsigned,
  table_id    int unsigned,
  name        varchar(255) charset 'utf8',
  charset     varchar(255),
  coltype     varchar(255),
  is_signed   tinyint(1) unsigned,
  enum_values text charset 'utf8',
  column_length tinyint unsigned,
  index (schema_id),
  index (table_id)
);

CREATE TABLE IF NOT EXISTS `positions` (
  server_id int unsigned not null,
  binlog_file varchar(255),
  binlog_position int unsigned,
  gtid_set varchar(4096),
  client_id varchar(255) charset latin1 not null default 'maxwell',
  heartbeat_at bigint null default null,
  last_heartbeat_read bigint null default null,
  primary key(server_id, client_id)
);
