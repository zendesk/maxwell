package com.zendesk.maxwell.core.producer;

import com.zendesk.maxwell.core.MaxwellContext;
import com.zendesk.maxwell.core.row.RowMap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
		String output = r.toJSON(outputConfig);

		if ( output != null ) {
			this.fileWriter.write(r.toJSON(outputConfig));
			this.fileWriter.write('\n');
			this.fileWriter.flush();
		}

		context.setPosition(r);
	}
}
