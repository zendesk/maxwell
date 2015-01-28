INSERT into sharded set account_id = 1, nice_id = 1, status_id = 2, date_field = '1979-10-10', text_field = 'Some Text', latin1_field = 'FooBarä', utf8_field = 'FooBarä', float_field = 1.333333333333, timestamp_field = '1980-01-01', decimal_field = 8.621
INSERT into sharded set account_id = 1, nice_id = 2, status_id = 2, date_field = '1979-10-10', text_field = 'Delete Me', latin1_field = 'FooBarä', utf8_field = 'FooBarä', float_field = 1.333333333333, timestamp_field = '1980-01-01', decimal_field = 8.621
FLUSH LOGS
UPDATE sharded set status_id = 1, text_field = 'Updated Text', timestamp_field=timestamp_field where id = 1
DELETE FROM sharded where nice_id = 2

DROP DATABASE IF EXISTS `shard_2`
CREATE DATABASE `shard_2`
