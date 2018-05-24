ALTER TABLE t1 ADD b GEOMETRY NOT NULL, ADD SPATIAL INDEX(b)
ALTER TABLE t1 ADD c POINT
CREATE TABLE IF NOT EXISTS slave_worker_info (   Id INTEGER UNSIGNED NOT NULL,    Relay_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,    Relay_log_pos BIGINT UNSIGNED NOT NULL,    Master_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,    Master_log_pos BIGINT UNSIGNED NOT NULL,    Checkpoint_relay_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,    Checkpoint_relay_log_pos BIGINT UNSIGNED NOT NULL,    Checkpoint_master_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,    Checkpoint_master_log_pos BIGINT UNSIGNED NOT NULL,    Checkpoint_seqno INT UNSIGNED NOT NULL,    Checkpoint_group_size INTEGER UNSIGNED NOT NULL,    Checkpoint_group_bitmap BLOB NOT NULL,    PRIMARY KEY(Id)) DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT 'Worker Information' ENGINE= INNODB
CREATE TABLE IF NOT EXISTS slave_worker_info (   Id INTEGER UNSIGNED NOT NULL,   Relay_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   Relay_log_pos BIGINT UNSIGNED NOT NULL,   Master_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   Master_log_pos BIGINT UNSIGNED NOT NULL,   Checkpoint_relay_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   Checkpoint_relay_log_pos BIGINT UNSIGNED NOT NULL,   Checkpoint_master_log_name TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   Checkpoint_master_log_pos BIGINT UNSIGNED NOT NULL,   Checkpoint_seqno INT UNSIGNED NOT NULL,   Checkpoint_group_size INTEGER UNSIGNED NOT NULL,   Checkpoint_group_bitmap BLOB NOT NULL,   PRIMARY KEY(Id)) DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT 'Worker Information' ENGINE= INNODB
CREATE TABLE `slave_worker_info` (   `Id` int(10) unsigned NOT NULL,   `Relay_log_name` text CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   `Relay_log_pos` bigint(20) unsigned NOT NULL,   `Master_log_name` text CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   `Master_log_pos` bigint(20) unsigned NOT NULL,   `Checkpoint_relay_log_name` text CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   `Checkpoint_relay_log_pos` bigint(20) unsigned NOT NULL,   `Checkpoint_master_log_name` text CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,   `Checkpoint_master_log_pos` bigint(20) unsigned NOT NULL,   `Checkpoint_seqno` int(10) unsigned NOT NULL,   `Checkpoint_group_size` int(10) unsigned NOT NULL,   `Checkpoint_group_bitmap` blob NOT NULL,   PRIMARY KEY (`Id`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 STATS_PERSISTENT=0 COMMENT='Worker Information'
CREATE TABLE `t3` (   `line` linestring NOT NULL )
CREATE TABLE gis_geometry (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g GEOMETRY)
CREATE TABLE gis_geometrycollection  (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g GEOMETRYCOLLECTION)
CREATE TABLE gis_line  (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g LINESTRING)
CREATE TABLE gis_multi_line (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g MULTILINESTRING)
CREATE TABLE gis_multi_point (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g MULTIPOINT)
CREATE TABLE gis_multi_polygon  (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g MULTIPOLYGON)
CREATE TABLE gis_point  (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g POINT)
CREATE TABLE gis_polygon   (fid INTEGER PRIMARY KEY AUTO_INCREMENT, g POLYGON)
CREATE TABLE t1 ( a INTEGER PRIMARY KEY AUTO_INCREMENT, gp  point, ln  linestring, pg  polygon, mp  multipoint, mln multilinestring, mpg multipolygon, gc  geometrycollection, gm  geometry )
CREATE TABLE t1 ( pk int, a varchar(1), b varchar(4), c tinyblob, d blob, e mediumblob, f longblob, g tinytext, h text, i mediumtext, j longtext, k geometry, PRIMARY KEY (pk) )
CREATE TABLE t1 (a GEOMETRY)
CREATE TABLE t1 (line LINESTRING NOT NULL) engine=myisam
CREATE TABLE t1 (name VARCHAR(100), square GEOMETRY)
CREATE TABLE t1 (p POINT)
CREATE TABLE t1(a LINESTRING NOT NULL, SPATIAL KEY(a))
CREATE TABLE t2 (line LINESTRING NOT NULL) engine=myisam
CREATE TABLE t2 (p POINT, INDEX(p))
CREATE TABLE test.t1_1 (f1 BIGINT, f2 TEXT, f2x TEXT, f3 CHAR(10), f3x CHAR(10), f4 BIGINT, f4x BIGINT, f5 POINT, f5x POINT NOT NULL) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci ENGINE = MyISAM
create table t1 (a int not null, b linestring not null, unique key b (b(12)))
create table t1 (a int not null, b linestring not null, unique key b (b(12)), unique key a (a))
create table t1 (pk integer primary key auto_increment, a geometry not null)
create table t1 (pk integer primary key auto_increment, fl geometry not null)
create table t1(City VARCHAR(30),Location geometry)
create table t1(a point)
