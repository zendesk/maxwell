CREATE TABLE `sharded` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `account_id` int(11) UNSIGNED NOT NULL,
  `nice_id` int(11) NOT NULL,
  `status_id` tinyint NOT NULL default 2,
  `date_field` datetime,
  `text_field` text,
  `latin1_field` varchar(96) CHARACTER SET latin1 NOT NULL DEFAULT '',
  `utf8_field` varchar(96) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `float_field` float(5,2),
  `timestamp_field` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `timestamp2_field` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `datetime2_field` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  -- Can't have a default time
  -- See http://dev.mysql.com/doc/refman/5.6/en/timestamp-initialization.html
  `time2_field` time(6),
  `decimal_field` decimal(12,7),
  PRIMARY KEY (`id`, `account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
