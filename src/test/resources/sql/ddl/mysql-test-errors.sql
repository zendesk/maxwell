ALTER TABLE ti1 CHECKSUM 1
ALTER TABLE tm1 CHECKSUM 1
CREATE TABLE "table_25930_b" (   """blah"" - 1" bigint(12) DEFAULT NULL )
CREATE TABLE IF NOT EXISTS help_relation ( help_topic_id int unsigned not null references help_topic, help_keyword_id  int unsigned not null references help_keyword, primary key (help_keyword_id, help_topic_id) ) engine=MyISAM CHARACTER SET utf8 comment='keyword-topic relation'
CREATE TABLE Product_Order (No INT NOT NULL AUTO_INCREMENT, Product_Category INT NOT NULL, Product_Id INT NOT NULL, Customer_Id INT NOT NULL, PRIMARY KEY(No), INDEX (Product_Category, Product_Id), FOREIGN KEY (Product_Category, Product_Id) REFERENCES Product(Category, Id) ON UPDATE CASCADE ON DELETE RESTRICT, INDEX (Customer_Id), FOREIGN KEY (Customer_Id) REFERENCES Customer(Id) ) ENGINE=INNODB
CREATE TABLE `t1` (id serial,intcol1 INT(32) ,intcol2 INT(32) ,charcol1 VARCHAR(128),charcol2 VARCHAR(128),charcol3 VARCHAR(128))
CREATE TABLE help_relation (   help_topic_id    int unsigned not null references help_topic,   help_keyword_id  int unsigned not null references help_keyword,   primary key      (help_keyword_id, help_topic_id) ) engine=MyISAM CHARACTER SET utf8   comment='keyword-topic relation'
CREATE TABLE t1 ( c01 BIT, c02 BIT(64), c03 TINYINT, c04 TINYINT UNSIGNED, c05 TINYINT ZEROFILL, c06 BOOL, c07 SMALLINT, c08 SMALLINT UNSIGNED, c09 SMALLINT ZEROFILL, c10 MEDIUMINT, c11 MEDIUMINT UNSIGNED, c12 MEDIUMINT ZEROFILL, c13 INT, c14 INT UNSIGNED, c15 INT ZEROFILL, c16 BIGINT, c17 BIGINT UNSIGNED, c18 BIGINT ZEROFILL, c19 FLOAT, c20 FLOAT UNSIGNED, c21 FLOAT ZEROFILL, c22 DOUBLE, c23 DOUBLE UNSIGNED, c24 DOUBLE ZEROFILL, c25 DECIMAL, c26 DECIMAL UNSIGNED, c27 DECIMAL ZEROFILL,
CREATE TABLE t1 ( c1 INT, c2 VARCHAR(300), KEY (c1) KEY_BLOCK_SIZE 1024, KEY (c2) KEY_BLOCK_SIZE 8192 )
CREATE TABLE t1(`FTS_DOC_ID` serial, no_fts_field VARCHAR(10), fts_field VARCHAR(10), FULLTEXT INDEX f(fts_field)) ENGINE=INNODB
CREATE TABLE t2(b1 INT, b2 INT, INDEX (b1, b2), CONSTRAINT A1 FOREIGN KEY (b1, b2) REFERENCES t1(a1, a2) ON UPDATE CASCADE ON DELETE NO ACTION) ENGINE=INNODB
CREATE TABLE t3(b1 INT, b2 INT, INDEX t3_indx (b1, b2), CONSTRAINT A2 FOREIGN KEY (b1, b2) REFERENCES t2(b1, b2) ON UPDATE SET NULL ON DELETE RESTRICT) ENGINE=INNODB
CREATE TABLE t4(b1 INT, b2 INT, UNIQUE KEY t4_ukey (b1, b2), CONSTRAINT A3 FOREIGN KEY (b1, b2) REFERENCES t3(b1, b2) ON UPDATE NO ACTION ON DELETE SET NULL) ENGINE=INNODB
CREATE TABLE t5(b1 INT, b2 INT, INDEX (b1, b2), CONSTRAINT A4 FOREIGN KEY (b1, b2) REFERENCES t4(b1, b2) ON UPDATE RESTRICT ON DELETE CASCADE) ENGINE=INNODB
alter table bug19145a alter column e set default null
alter table bug19145a alter column s set default null
alter table bug19145b alter column e set default null
alter table bug19145b alter column s set default null
alter table t1 add f2 enum(0xFFFF)
create table t1 ( min_num   dec(6,6)     default .000001)
create table t1 ( min_num   dec(6,6)     default 0.000001)
create table t1 ("t1 column" int)
create table t1 (a enum(0xE4, '1', '2') not null default 0xE4)
create table t1 (a int check (a>0))
create table t1 (c nchar varying(10))
create table t1 (t1.index int)
create table t1(a char character set cp1251 default _koi8r 0xFF)
create table t1(c enum(0x9353,0x9373) character set sjis)
create table t1(t1.name int)
create table t2 as select * from t1
create table t2(test.t2.name int)
create table test.no_index_tab ( a varchar(255) not null, b int not null) engine = merge union = (test.no_index_tab_1,test.no_index_tab_2) insert_method = first
