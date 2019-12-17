CREATE TABLE IF NOT EXISTS `bootstrap` (
  id              int unsigned auto_increment NOT NULL primary key,
  database_name   varchar(255) charset 'utf8' NOT NULL,
  table_name      varchar(255) charset 'utf8' NOT NULL,
  where_clause    text default NULL,
  is_complete     tinyint(1) unsigned NOT NULL default 0,
  inserted_rows   bigint(20) unsigned NOT NULL DEFAULT 0,
  total_rows      bigint(20) unsigned NOT NULL DEFAULT 0,
  created_at      DATETIME default NULL,
  started_at      DATETIME default NULL,
  completed_at    DATETIME default NULL,
  binlog_file     varchar(255) default NULL,
  binlog_position int unsigned default 0,
  client_id       varchar(255) charset 'latin1' not null default 'maxwell',
  comment         varchar(255) charset 'utf8' default NULL
);
