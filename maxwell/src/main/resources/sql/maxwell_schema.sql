CREATE TABLE IF NOT EXISTS `schemas` (
  id int unsigned auto_increment NOT NULL primary key,
  binlog_file varchar(255),
  binlog_position int unsigned,
  server_id int unsigned,
  charset varchar(255),
  deleted tinyint(1) not null default 0
)ENGINE=innodb;

CREATE TABLE IF NOT EXISTS `databases` (
  id        int unsigned auto_increment NOT NULL primary key,
  schema_id int unsigned,
  name      varchar(255),
  charset   varchar(255),
  index (schema_id)
)ENGINE=innodb;

CREATE TABLE IF NOT EXISTS `tables` (
  id          int unsigned auto_increment NOT NULL primary key,
  schema_id   int unsigned,
  database_id int unsigned,
  name      varchar(255),
  charset   varchar(255),
  pk        varchar(1024) charset 'utf8',
  index (schema_id),
  index (database_id)
)ENGINE=innodb;

CREATE TABLE IF NOT EXISTS `columns` (
  id          int unsigned auto_increment NOT NULL primary key,
  schema_id   int unsigned,
  table_id    int unsigned,
  name        varchar(255),
  charset     varchar(255),
  coltype     varchar(255),
  is_signed   tinyint(1) unsigned,
  enum_values text,
  index (schema_id),
  index (table_id)
)ENGINE=innodb;

CREATE TABLE IF NOT EXISTS `positions` (
  server_id int unsigned not null primary key,
  binlog_file varchar(255),
  binlog_position int unsigned
)ENGINE=innodb;

CREATE TABLE `bootstrap` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `database_name` varchar(255) NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `is_complete` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `inserted_rows` bigint(20) unsigned NOT NULL DEFAULT '0',
  `total_rows` bigint(20) unsigned NOT NULL DEFAULT '0',
  `created_at` datetime DEFAULT NULL,
  `started_at` datetime DEFAULT NULL,
  `completed_at` datetime DEFAULT NULL,
  `binlog_file` varchar(255) DEFAULT NULL,
  `binlog_position` int(10) unsigned DEFAULT '0',
  PRIMARY KEY (`id`)
)  ENGINE=innodb;
