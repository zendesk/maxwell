ALTER TABLE ti1 CHECKSUM 1
ALTER TABLE tm1 CHECKSUM 1
CREATE TABLE "table_25930_b" (   """blah"" - 1" bigint(12) DEFAULT NULL )
CREATE TABLE t1 ( c01 BIT, c02 BIT(64), c03 TINYINT, c04 TINYINT UNSIGNED, c05 TINYINT ZEROFILL, c06 BOOL, c07 SMALLINT, c08 SMALLINT UNSIGNED, c09 SMALLINT ZEROFILL, c10 MEDIUMINT, c11 MEDIUMINT UNSIGNED, c12 MEDIUMINT ZEROFILL, c13 INT, c14 INT UNSIGNED, c15 INT ZEROFILL, c16 BIGINT, c17 BIGINT UNSIGNED, c18 BIGINT ZEROFILL, c19 FLOAT, c20 FLOAT UNSIGNED, c21 FLOAT ZEROFILL, c22 DOUBLE, c23 DOUBLE UNSIGNED, c24 DOUBLE ZEROFILL, c25 DECIMAL, c26 DECIMAL UNSIGNED, c27 DECIMAL ZEROFILL,
CREATE TABLE t1 ( c1 INT, c2 VARCHAR(300), KEY (c1) KEY_BLOCK_SIZE 1024, KEY (c2) KEY_BLOCK_SIZE 8192 )
alter table t1 add f2 enum(0xFFFF)
create table t1 ( min_num   dec(6,6)     default .000001)
create table t1 ( min_num   dec(6,6)     default 0.000001)
create table t1 ("t1 column" int)
create table t1 (a enum(0xE4, '1', '2') not null default 0xE4)
create table t1 (c nchar varying(10))
create table t1 (t1.index int)
create table t1(a char character set cp1251 default _koi8r 0xFF)
create table t1(c enum(0x9353,0x9373) character set sjis)
create table t1(t1.name int)
create table t2 as select * from t1
create table t2(test.t2.name int)
create table test.no_index_tab ( a varchar(255) not null, b int not null) engine = merge union = (test.no_index_tab_1,test.no_index_tab_2) insert_method = first
