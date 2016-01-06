SET @c = CONCAT('CREATE DATABASE IF NOT EXISTS ', @maxwell_db)

PREPARE maxwell_stmt from @c

EXECUTE maxwell_stmt

DEALLOCATE PREPARE maxwell_stmt

SET @c = CONCAT('CREATE TABLE IF NOT EXISTS `', @maxwell_db, '`.`schemas` (
  id int unsigned auto_increment NOT NULL primary key,
  binlog_file varchar(255),
  binlog_position int unsigned,
  server_id int unsigned,
  encoding varchar(255),
  deleted tinyint(1) not null default 0
  )')

PREPARE maxwell_stmt from @c

EXECUTE maxwell_stmt

DEALLOCATE PREPARE maxwell_stmt

SET @c = CONCAT('CREATE TABLE IF NOT EXISTS `', @maxwell_db, '`.`databases` (
  id        int unsigned auto_increment NOT NULL primary key,
  schema_id int unsigned,
  name      varchar(255),
  encoding  varchar(255),
  index (schema_id)
  )')

PREPARE maxwell_stmt from @c

EXECUTE maxwell_stmt

DEALLOCATE PREPARE maxwell_stmt

SET @c = CONCAT('CREATE TABLE IF NOT EXISTS `', @maxwell_db, '`.`tables` (
  id          int unsigned auto_increment NOT NULL primary key,
  schema_id   int unsigned,
  database_id int unsigned,
  name      varchar(255),
  encoding  varchar(255),
  pk        varchar(1024) charset ''utf8'',
  index (schema_id),
  index (database_id)
  )')

PREPARE maxwell_stmt from @c

EXECUTE maxwell_stmt

DEALLOCATE PREPARE maxwell_stmt

SET @c = CONCAT('CREATE TABLE IF NOT EXISTS `', @maxwell_db, '`.`columns` (
  id          int unsigned auto_increment NOT NULL primary key,
  schema_id   int unsigned,
  table_id    int unsigned,
  name        varchar(255),
  encoding    varchar(255),
  coltype     varchar(255),
  is_signed   tinyint(1) unsigned,
  enum_values text,
  index (schema_id),
  index (table_id)
  )')

PREPARE maxwell_stmt from @c

EXECUTE maxwell_stmt

DEALLOCATE PREPARE maxwell_stmt

SET @c = CONCAT('CREATE TABLE IF NOT EXISTS `', @maxwell_db, '`.`positions` (
  server_id int unsigned not null primary key,
  binlog_file varchar(255),
  binlog_position int unsigned
  )')

PREPARE maxwell_stmt from @c

EXECUTE maxwell_stmt

DEALLOCATE PREPARE maxwell_stmt
