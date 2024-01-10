# Basic Filters
***

Maxwell can be configured to filter out updates from specific tables.  This is controlled
by the `--filter` command line flag.

## Example 1

```
--filter = 'exclude: foodb.*, include: foodb.tbl, include: foodb./table_\d+/'
```

This example tells Maxwell to suppress all updates that happen on `foodb`, except for updates
to `tbl` and any table in foodb matching the regexp `/table_\d+/`.
## Example 2

Filter options are evaluated in the order specified, so in this example we
suppress everything except updates in the `db1` database.

```
--filter = 'exclude: *.*, include: db1.*'
```


# Column Filters
***
Maxwell can also include/exclude based on column values:

```
--filter = 'exclude: db.tbl.col = reject'
```

will reject any row in `db.tbl` that contains `col` and where the stringified value of "col" is "reject".
Column filters are ignored if the specified column is not present, so `--filter = 'exclude: *.*.col_a = *'`
will exclude updates to any table that contains `col_a`, but include every other table.


# Blacklisting
***
In rare cases, you may wish to tell Maxwell to completely ignore a database or
table, including schema changes.  In general, don't use this.  If you must use this:

```
--filter = 'blacklist: bad_db.*'
```

Note that once Maxwell has been running with a table or database marked as
blacklisted, you *must* continue to run Maxwell with that table or database
blacklisted or else Maxwell will halt. If you want to stop
blacklisting a table or database, you will have to drop the maxwell schema first.
Also note that this is the feature I most regret writing.


# Javascript Filters
***
If you need more flexibility than the native filters provide, you can write a small chunk of
javascript for Maxwell to pass each row through with `--javascript FILE`.  This file should contain
at least a javascript function named `process_row`.  This function will be passed a [`WrappedRowMap`]()
object that represents the current row and a [`LinkedHashMap<String, Object>`]() which represents a global state and is free to make filtering and data munging decisions:

```
function process_row(row, state) {
	// Updating the state object
	if ( row.database == "test" && row.table == "lock") {
		var haslock = row.data.get("haslock");
		if ( haslock == "false" ) {
			state.put("haslock", "false");
		} else if( haslock == "true" ) {
			state.put("haslock", "true");
		}
	}

	// Suppressing rows based on state
	if(state.get("haslock") == "true") {
		row.suppress();
	}

	// Filter and Change based on actual data
	if ( row.database == "test" && row.table == "bar" ) {
		var username = row.data.get("username");
		if ( username == "osheroff" )
			row.suppress();

		row.data.put("username", username.toUpperCase());
	}
}
```

There's a longer example here: [https://github.com/zendesk/maxwell/blob/master/src/example/filter.js](https://github.com/zendesk/maxwell/blob/master/src/example/filter.js).

