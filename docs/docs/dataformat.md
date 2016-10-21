<h4>How Maxwell translates different mysql types</h4>

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

As of 1.4.0, Maxwell supports microsecond precision datetime/timestamp/time columns.

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

charset  | status
---------| ------
utf8     | supported
utf8mb4  | supported
latin1   | supported
latin2   | supported
ascii    | supported
ucs2     | supported
binary   | supported (as base64)
utf16    | supported, not tested in production
utf32    | supported, not tested in production
big5     | supported, not tested in production
cp850    | supported, not tested in production
sjis     | supported, not tested in production
hebrew   | supported, not tested in production
tis620   | supported, not tested in production
euckr    | supported, not tested in production
gb2312   | supported, not tested in production
greek    | supported, not tested in production
cp1250   | supported, not tested in production
gbk      | supported, not tested in production
latin5   | supported, not tested in production
macroman | supported, not tested in production
cp852    | supported, not tested in production
cp1251   | supported, not tested in production
cp866    | supported, not tested in production
cp1256   | supported, not tested in production
cp1257   | supported, not tested in production
dec8     | unsupported
hp8      | unsupported
koi8r    | unsupported
swe7     | unsupported
ujis     | unsupported
koi8u    | unsupported
armscii8 | unsupported
keybcs2  | unsupported
macce    | unsupported
latin7   | unsupported
geostd8  | unsupported
cp932    | unsupported
eucjpms  | unsupported


<script>
  jQuery(document).ready(function () {
    jQuery("table").addClass("table table-condensed table-bordered table-hover");
  });
</script>
