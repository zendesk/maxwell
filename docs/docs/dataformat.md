### So you ran some sql?

    create table test.e (
      id int(10) not null primary key auto_increment,
      m double,
      c timestamp(6),
      comment varchar(255) charset 'latin1'
    );

    insert into test.e set m = 4.2341, c = now(3), comment = 'I am a creature of light.';
    update test.e set m = 5.444, c = now(3) where id = 1;
    delete from test.e where id = 1;
    alter table test.e add column torvalds bigint unsigned after m;
    drop table test.e;

Maxwell will produce some output for that.  Let's look at it.

### INSERT
***

```
mysql> insert into test.e set m = 4.2341, c = now(3), comment = 'I am a creature of light.';
{
   "database":"test",
   "table":"e",
   "type":"insert",
   "ts":1477053217,
   "xid":23396,
   "commit":true,
   "position":"master.000006:800911",
   "server_id":23042,
   "thread_id":108,
   "primary_key": [1, "2016-10-21 05:33:37.523000"],
   "primary_key_columns": ["id", "c"],
   "data":{
      "id":1,
      "m":4.2341,
      "c":"2016-10-21 05:33:37.523000",
      "comment":"I am a creature of light."
   }
}

```

Most of the fields are self-explanatory, but a couple of them deserve mention:

↳ `"type":"insert",`

Most commonly you will see insert/update/delete here.  If you're bootstrapping
a table, you will see "bootstrap-insert", and DDL statements (explained later)
have their own types.

↳ `"xid":23396,`

This is InnoDB's "transaction ID" for the transaction this row is associated
with.  It's unique within the lifetime of a server as near as I can tell.

↳ `"server_id":23042,`

The mysql server_id of the server that accepted this transaction.


↳ `"thread_id":108,`

A thread_id is more or less a unique identifier of the client connection that
generated the data.

↳ `"commit":true,`

If you need to re-assemble transactions in your stream processors, you can use
this field and `xid` to do so.  The data will look like:

- row with no `commit`, xid=142
- row with no `commit`, xid=142
- row with `commit=true`, xid=142
- row with no `commit`, xid=155
- ...

↳ `"primary_key": [1,"2016-10-21 05:33:37.523000"],`

You only get this with --output_primary_key. List of values that make up the
primary key for this row.

↳ `"primary_key_columns": ["id","c"],`

You only get this with --output_primary_key_columns. List of columns that make
make up the primary key for this row.


### UPDATE
***

```
mysql> update test.e set m = 5.444, c = now(3) where id = 1;
{
   "database":"test",
   "table":"e",
   "type":"update",
   "ts":1477053234,
   ...
   "data":{
      "id":1,
      "m":5.444,
      "c":"2016-10-21 05:33:54.631000",
      "comment":"I am a creature of light."
   },
   "old":{
      "m":4.2341,
      "c":"2016-10-21 05:33:37.523000"
   }
}

```


What's important to note here is the `old` field, which stores old values for
rows that changed.  So `data` still has a complete copy of the row (just as
with the insert), but now you can reconstruct what the row *was* by doing
`data.merge(old)`.

### DELETE
***

```
mysql> delete from test.e where id = 1;
{
   "database":"test",
   "table":"e",
   "type":"delete",
   ...
   "data":{
      "id":1,
      "m":5.444,
      "c":"2016-10-21 05:33:54.631000",
      "comment":"I am a creature of light."
   }
}
```

after a DELETE, `data` contains a copy of the row, just before it shuffled off
this mortal coil.


### CREATE TABLE
***

```
create table test.e ( ... )
{
   "type":"table-create",
   "database":"test",
   "table":"e",
   "def":{
      "database":"test",
      "charset":"utf8mb4",
      "table":"e",
      "columns":[
         {
            "type":"int",
            "name":"id",
            "signed":true
         },
         {
            "type":"double",
            "name":"m"
         },
         {
            "type":"timestamp",
            "name":"c",
            "column-length":6
         },
         {
            "type":"varchar",
            "name":"comment",
            "charset":"latin1"
         }
      ],
      "primary-key":[
         "id"
      ]
   },
   "ts":1477053126000,
   "sql":"create table test.e ( id int(10) not null primary key auto_increment, m double, c timestamp(6), comment varchar(255) charset 'latin1' )",
   "position":"master.000006:800050"
}

```

You only get this with `--output_ddl`.

↳ ` "type": "table-create" `
here you have `database-create`, `database-alter`, `database-drop`, `table-create`, `table-alter`, `table-drop`.

↳ `"type":"int",`
Mostly here we preserve the inbound type of the column.  There's a couple of
exceptions where we will change the column type, you could read about them in the
[unalias_type](https://github.com/zendesk/maxwell/blob/master/src/main/java/com/zendesk/maxwell/schema/columndef/ColumnDef.java#L109)
function if you so desired.

### ALTER TABLE

```
mysql> alter table test.e add column torvalds bigint unsigned after m;
{
   "type":"table-alter",
   "database":"test",
   "table":"e",
   "old":{
      "database":"test",
      "charset":"utf8mb4",
      "table":"e",
      "columns":[
         {
            "type":"int",
            "name":"id",
            "signed":true
         },
         {
            "type":"double",
            "name":"m"
         },
         {
            "type":"timestamp",
            "name":"c",
            "column-length":6
         },
         {
            "type":"varchar",
            "name":"comment",
            "charset":"latin1"
         }
      ],
      "primary-key":[
         "id"
      ]
   },
   "def":{
      "database":"test",
      "charset":"utf8mb4",
      "table":"e",
      "columns":[
         {
            "type":"int",
            "name":"id",
            "signed":true
         },
         {
            "type":"double",
            "name":"m"
         },
         {
            "type":"bigint",
            "name":"torvalds",
            "signed":false
         },
         {
            "type":"timestamp",
            "name":"c",
            "column-length":6
         },
         {
            "type":"varchar",
            "name":"comment",
            "charset":"latin1"
         }
      ],
      "primary-key":[
         "id"
      ]
   },
   "ts":1477053308000,
   "sql":"alter table test.e add column torvalds bigint unsigned after m",
   "position":"master.000006:804398"
}
```

As with the CREATE TABLE, we have a complete image of the table before-and-after the alter


### blob (+ binary encoded strings)
***

Maxell will base64 encode BLOB, BINARY and VARBINARY columns (as well as varchar/string columns with a BINARY encoding).


### datetime
***
Datetime columns are output as "YYYY-MM-DD hh:mm::ss" strings.  Note that mysql
has no problem storing invalid datetimes like "0000-00-00 00:00:00", and
Maxwell chooses to reproduce these invalid datetimes faithfully,
for lack of something better to do.


```
mysql>    create table test_datetime ( id int(11), dtcol datetime );
mysql>    insert into test_datetime set dtcol='0000-00-00 00:00:00';

<maxwell  { "table" : "test_datetime", "type": "insert", "data": { "dtcol": "0000-00-00 00:00:00" } }
```

As of 1.3.0, Maxwell supports microsecond precision datetime/timestamp/time columns.

### sets
***

output as JSON arrays.

```
mysql>   create table test_sets ( id int(11), setcol set('a_val', 'b_val', 'c_val') );
mysql>   insert into test_sets set setcol = 'b_val,c_val';

<maxwell { "table":"test_sets", "type":"insert", "data": { "setcol": ["b_val", "c_val"] } }
```

### strings (varchar, text)
***
Maxwell will accept a variety of character encodings, but will always output UTF-8 strings.  The following table
describes support for mysql's character sets:

| charset  | status                              |
|:---------|:------------------------------------|
| utf8     | supported                           |
| utf8mb4  | supported                           |
| latin1   | supported                           |
| latin2   | supported                           |
| ascii    | supported                           |
| ucs2     | supported                           |
| binary   | supported (as base64)               |
| utf16    | supported, not tested in production |
| utf32    | supported, not tested in production |
| big5     | supported, not tested in production |
| cp850    | supported, not tested in production |
| sjis     | supported, not tested in production |
| hebrew   | supported, not tested in production |
| tis620   | supported, not tested in production |
| euckr    | supported, not tested in production |
| gb2312   | supported, not tested in production |
| greek    | supported, not tested in production |
| cp1250   | supported, not tested in production |
| gbk      | supported, not tested in production |
| latin5   | supported, not tested in production |
| macroman | supported, not tested in production |
| cp852    | supported, not tested in production |
| cp1251   | supported, not tested in production |
| cp866    | supported, not tested in production |
| cp1256   | supported, not tested in production |
| cp1257   | supported, not tested in production |
| dec8     | unsupported                         |
| hp8      | unsupported                         |
| koi8r    | unsupported                         |
| swe7     | unsupported                         |
| ujis     | unsupported                         |
| koi8u    | unsupported                         |
| armscii8 | unsupported                         |
| keybcs2  | unsupported                         |
| macce    | unsupported                         |
| latin7   | unsupported                         |
| geostd8  | unsupported                         |
| cp932    | unsupported                         |
| eucjpms  | unsupported                         |
