CREATE TABLE `sharded` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `account_id` int(11) NOT NULL,
  `nice_id` int(11) NOT NULL,
  `status_id` tinyint NOT NULL default 2,
  `date_field` datetime,
  `text_field` text, 
  `latin1_field` varchar(96) CHARACTER SET latin1 NOT NULL,
  `utf8_field` varchar(96) CHARACTER SET utf8 NOT NULL,
  `float_field` float(5,2), 
  `timestamp_field` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
