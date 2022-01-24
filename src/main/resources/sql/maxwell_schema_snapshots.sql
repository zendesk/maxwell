CREATE TABLE IF NOT EXISTS `snapshots` (
 `id` char(36) CHARACTER SET ascii NOT NULL,
 `database` varchar(255) CHARACTER SET utf8 NOT NULL,
 `table` varchar(255) CHARACTER SET utf8 NOT NULL,
 `where_clause` text,
 `watermark` varchar(36) CHARACTER SET ascii NULL,
 `chunk_start` int(10) unsigned DEFAULT '0',
 `rows_sent` int(10) unsigned DEFAULT '0',
 `complete` tinyint(1) unsigned NOT NULL DEFAULT '0',
 `successful` tinyint(1) unsigned NOT NULL DEFAULT '0',
 `created_at` DATETIME NOT NULL,
 `completed_at` DATETIME DEFAULT NULL,
 `request_comment` varchar(255) CHARACTER SET utf8 NULL,
 `completion_reason` varchar(255) CHARACTER SET utf8 NULL,
 `client_id` varchar(255) charset latin1 not null default 'maxwell',
 INDEX (complete),
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
