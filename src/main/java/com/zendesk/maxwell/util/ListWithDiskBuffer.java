package com.zendesk.maxwell.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/*
   a wrapper class for a linked list that will keep N tail elements
   in memory, spilling its head onto disk as needed.
 */
public class ListWithDiskBuffer<T extends Serializable> {
	static final Logger LOGGER = LoggerFactory.getLogger(ListWithDiskBuffer.class);
	private final long maxInMemoryElements;
	private final LinkedList<T> list;
	private long elementsInFile = 0;
	private File file;
	private ObjectInputStream is;
	private ObjectOutputStream os;

	public ListWithDiskBuffer(long maxInMemoryElements) throws IOException {
		this.maxInMemoryElements = maxInMemoryElements;
		list = new LinkedList<>();
	}

	public void add(T element) throws IOException {
		list.add(element);

		while (this.list.size() > maxInMemoryElements) {
			if ( file == null ) {
				file = File.createTempFile("maxwell", "events");
				file.deleteOnExit();
				os = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			}

			if ( elementsInFile == 0 )
				LOGGER.debug("Overflowed in-memory buffer, spilling over into " + file);

			os.writeObject(this.list.removeFirst());

			elementsInFile++;

			if ( elementsInFile % maxInMemoryElements == 0 )
				os.reset(); // flush ObjectOutputStream caches
		}
	}

	public boolean isEmpty() {
		return this.size() == 0;
	}

	public T getLast() {
		return list.getLast();
	}

	public T removeFirst() throws IOException, ClassNotFoundException {
		if ( elementsInFile > 0 ) {
			if ( is == null ) {
				os.flush();
				is = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
			}

			T element = (T) is.readObject();
			elementsInFile--;
			return element;
		} else {
			return list.removeFirst();
		}
	}

	public Long size() {
		return list.size() + elementsInFile;
	}

	public Long inMemorySize() {
		return Long.valueOf(list.size());
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			file.delete();
		} finally {
			super.finalize();
		}
	}
}
