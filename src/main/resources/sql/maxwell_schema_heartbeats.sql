CREATE TABLE IF NOT EXISTS `heartbeats` (
  server_id int unsigned not null,
  client_id varchar(255) charset latin1 not null default 'maxwell',
  heartbeat bigint not null,
  primary key(server_id, client_id)
);
