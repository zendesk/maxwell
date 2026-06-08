package com.zendesk.maxwell.replication;

import com.github.luben.zstd.Zstd;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Deserializes {@code TRANSACTION_PAYLOAD} events, emitted when MySQL is configured with
 * {@code binlog_transaction_compression=ON} (MySQL 8.0.20+).
 * <p>
 * The stock {@code com.github.shyiko...TransactionPayloadEventDataDeserializer} decompresses the
 * zstd payload but parses the inner events with a <em>default</em> {@link EventDeserializer} that
 * lacks Maxwell's compatibility modes. That makes datetime values lose sub-millisecond precision
 * and, worse, makes binary/non-utf8 columns decode incorrectly (a {@code byte[]} that Maxwell would
 * Base64/charset-decode arrives as an already-(mis)decoded {@code String}). This drop-in replacement
 * parses the inner events with the same compatibility modes Maxwell applies to uncompressed events,
 * so a compressed transaction yields the same RowMaps as an uncompressed one.
 * <p>
 * Registered for {@link com.github.shyiko.mysql.binlog.event.EventType#TRANSACTION_PAYLOAD} in
 * {@link BinlogConnectorReplicator}. The inner events it produces are surfaced to the replication
 * queue by {@link BinlogConnectorEventListener#onEvent}, which the upstream client does not do.
 */
public class MaxwellTransactionPayloadDeserializer implements EventDataDeserializer<TransactionPayloadEventData> {
	// On-the-wire payload header field types (MySQL's Transaction_payload_event format).
	private static final int OTW_PAYLOAD_HEADER_END_MARK = 0;
	private static final int OTW_PAYLOAD_SIZE_FIELD = 1;
	private static final int OTW_PAYLOAD_COMPRESSION_TYPE_FIELD = 2;
	private static final int OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3;

	// MySQL currently defines only ZSTD (0) and NONE (255); zstd is the only compressed form.
	private static final int COMPRESSION_TYPE_ZSTD = 0;

	private final EventDeserializer.CompatibilityMode[] compatibilityModes;

	public MaxwellTransactionPayloadDeserializer(EventDeserializer.CompatibilityMode... compatibilityModes) {
		this.compatibilityModes = compatibilityModes;
	}

	@Override
	public TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
		TransactionPayloadEventData eventData = new TransactionPayloadEventData();

		// Payload header: a sequence of (type, length, value) triplets terminated by an
		// end-mark field. deserializeEventData() has already bounded inputStream to the event
		// body (checksum excluded), so available() tracks the remaining header+payload bytes.
		while (inputStream.available() > 0) {
			int fieldType = inputStream.readPackedInteger();
			if (fieldType == OTW_PAYLOAD_HEADER_END_MARK) {
				break;
			}
			int fieldLength = inputStream.readPackedInteger();
			switch (fieldType) {
				case OTW_PAYLOAD_SIZE_FIELD:
					eventData.setPayloadSize(inputStream.readPackedInteger());
					break;
				case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
					eventData.setCompressionType(inputStream.readPackedInteger());
					break;
				case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
					eventData.setUncompressedSize(inputStream.readPackedInteger());
					break;
				default:
					inputStream.read(fieldLength);
					break;
			}
		}

		if (eventData.getUncompressedSize() == 0) {
			eventData.setUncompressedSize(eventData.getPayloadSize());
		}

		eventData.setPayload(inputStream.read(eventData.getPayloadSize()));

		if (eventData.getCompressionType() != COMPRESSION_TYPE_ZSTD) {
			throw new IOException("Unsupported binlog_transaction_compression type: "
				+ eventData.getCompressionType() + " (only ZSTD is supported)");
		}

		byte[] compressed = eventData.getPayload();
		byte[] decompressed = ByteBuffer.allocate(eventData.getUncompressedSize()).array();
		long rc = Zstd.decompressByteArray(decompressed, 0, decompressed.length, compressed, 0, compressed.length);
		if (Zstd.isError(rc)) {
			throw new IOException("Failed to zstd-decompress binlog transaction payload: " + Zstd.getErrorName(rc));
		}

		// Parse the decompressed inner events with Maxwell's compatibility modes. A single
		// deserializer instance handles the whole payload so TABLE_MAP events register their
		// column metadata before the row events that reference them. Inner events carry no
		// checksum, which matches the default (NONE) on a fresh deserializer.
		EventDeserializer eventDeserializer = new EventDeserializer();
		if (compatibilityModes.length > 0) {
			eventDeserializer.setCompatibilityMode(
				compatibilityModes[0],
				Arrays.copyOfRange(compatibilityModes, 1, compatibilityModes.length)
			);
		}

		ArrayList<Event> events = new ArrayList<>();
		ByteArrayInputStream uncompressedStream = new ByteArrayInputStream(decompressed);
		for (Event event = eventDeserializer.nextEvent(uncompressedStream);
			 event != null;
			 event = eventDeserializer.nextEvent(uncompressedStream)) {
			events.add(event);
		}
		eventData.setUncompressedEvents(events);

		return eventData;
	}
}
