CREATE TABLE IF NOT EXISTS `maxwell`.`bootstrap` (
  id              int unsigned auto_increment NOT NULL primary key,
  database_name   varchar(255) NOT NULL,
  table_name      varchar(255) NOT NULL,
  is_complete     tinyint(1) unsigned NOT NULL default 0,
  inserted_rows   int unsigned NOT NULL default 0,
  created_at      DATETIME default NULL,
  started_at      DATETIME default NULL,
  completed_at    DATETIME default NULL,
  binlog_file varchar(255) default NULL,
  binlog_position int unsigned default 0
);

