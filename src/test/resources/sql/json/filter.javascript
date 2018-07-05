function process_row(rowmap) {
	if ( rowmap.data.a ) {
		if ( rowmap.data.a.match(/xyzzy/) )
			rowmap.suppress();

		rowmap.data.a = rowmap.data.a.toUpperCase();
	}
}
