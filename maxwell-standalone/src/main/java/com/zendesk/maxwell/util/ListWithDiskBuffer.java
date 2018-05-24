package com.zendesk.maxwell.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/*
   a wrapper class for a linked list that will keep N tail elements
   in memory, spilling its head onto disk as needed.
 */
public class ListWithDiskBuffer<T> {
	static final Logger LOGGER = LoggerFactory.getLogger(ListWithDiskBuffer.class);
	private final long maxInMemoryElements;
	private final LinkedList<T> list;
	private long elementsInFile = 0;
	private File file;
	private ObjectInputStream is;
	private ObjectOutputStream os;

	public ListWithDiskBuffer(long maxInMemoryElements) {
		this.maxInMemoryElements = maxInMemoryElements;
		list = new LinkedList<>();
	}

	public void add(T element) throws IOException {
		list.add(element);

		while ( shouldBuffer() )
			evict();
	}

	protected boolean shouldBuffer() {
		return this.list.size() > maxInMemoryElements;
	}

	protected void resetOutputStreamCaches() throws IOException {
		os.reset();
	}

	public void flushToDisk() throws IOException {
		if ( os != null )
			os.flush();
	}

	public boolean isEmpty() {
		return this.size() == 0;
	}

	public T getLast() {
		return list.getLast();
	}

	public T removeFirst(Class<T> clazz) throws IOException, ClassNotFoundException {
		if ( elementsInFile > 0 ) {
			if ( is == null ) {
				os.flush();
				is = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
			}

			Object object = is.readObject();
			T element = clazz.cast(object);
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
			if ( file != null )
				file.delete();
		} finally {
			super.finalize();
		}
	}

	protected T evict() throws IOException {
		if ( file == null ) {
			file = File.createTempFile("maxwell", "events");
			file.deleteOnExit();
			os = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
		}

		if ( elementsInFile == 0 )
			LOGGER.info("Overflowed in-memory buffer, spilling over into " + file);

		T evicted = this.list.removeFirst();
		os.writeObject(evicted);

		elementsInFile++;

		if ( elementsInFile % maxInMemoryElements == 0 )
			resetOutputStreamCaches();

		return evicted;
	}

}
