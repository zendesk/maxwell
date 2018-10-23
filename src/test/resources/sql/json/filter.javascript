function process_row(rowmap) {
	if ( rowmap.data.a ) {
		if ( rowmap.data.a.match(/xyzzy/) )
			rowmap.suppress();

		rowmap.data.a = rowmap.data.a.toUpperCase();
	}

	if ( rowmap.query && rowmap.query.match(/mangle/) ) {
		rowmap.query = "mangled";
	}
}
