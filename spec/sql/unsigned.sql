CREATE TABLE `ints` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `i64` bigint UNSIGNED,
  `i32` int UNSIGNED,
  `i24` mediumint UNSIGNED,
  `i16` smallint UNSIGNED,
  `i8`  tinyint  UNSIGNED,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

