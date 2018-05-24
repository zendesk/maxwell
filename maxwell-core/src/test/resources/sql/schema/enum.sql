CREATE TABLE IF NOT EXISTS `test`.`enum_test` (
  `language` ENUM('en-US', 'de-DE'),
  `decimal_separator` ENUM(',', '.'),
  PRIMARY KEY (`language`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
