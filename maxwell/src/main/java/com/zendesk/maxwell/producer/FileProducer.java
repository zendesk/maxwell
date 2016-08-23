package com.zendesk.maxwell.producer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.zendesk.maxwell.MaxwellAbstractRowsEvent;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;

public class FileProducer extends AbstractProducer {
	private final File file;
	private final FileWriter fileWriter;

	public FileProducer(MaxwellContext context, String filename) throws IOException {
		super(context);
		this.file = new File(filename);
		this.fileWriter = new FileWriter(this.file, true);
	}

	@Override
	public void push(RowMap r) throws Exception {
		this.fileWriter.write(r.toJSON());
		this.fileWriter.write('\n');
		this.fileWriter.flush();

		context.setPosition(r);
	}
}
