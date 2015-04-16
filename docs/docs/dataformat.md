<div class="content-title">How Maxwell munges various datatypes</div>

***

#### strings (varchar, text)

Maxwell currently supports latin1 and utf-8 columns, and will convert both to UTF-8 before outputting as JSON.

***

#### blob (+ binary encoded strings)

Not currently well-supported.  The plan of record is to covert blobs to Base64 strings, and to leave hints
in the JSON that they're blobs.  One strong argument for getting avro schemas supported, I guess.

***

#### datetime

Datetime columns are output as "YYYY-MM-DD hh:mm::ss" strings.  Note that mysql
has no problem storing invalid datetimes like "0000-00-00 00:00:00", and
Maxwell chooses to reproduce these invalid datetimes faithfully,
for lack of something better to do.


```
mysql>    create table test_datetime ( id int(11), dtcol datetime );
mysql>    insert into test_datetime set dtcol='0000-00-00 00:00:00';

<maxwell  {"table":"test_datetime","type":"insert","data":{"dtcol":"0000-00-00 00:00:00"}}
```

***

#### sets

output as JSON arrays.

```
mysql>   create table test_sets ( id int(11), setcol set('a_val', 'b_val', 'c_val') );
mysql>   insert into test_sets set setcol = 'b_val,c_val';

<maxwell {"table":"test_sets","type":"insert","data":{"setcol":["b_val","c_val"]}}
```


