CREATE TABLE `minimal` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `account_id` int(11) NOT NULL,
  `text_field` varchar(96),
  PRIMARY KEY (id, text_field)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

