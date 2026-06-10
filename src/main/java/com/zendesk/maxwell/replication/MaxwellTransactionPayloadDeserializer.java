package com.zendesk.maxwell.replication;

import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdException;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

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
 * so a compressed transaction yields the same RowMaps as an uncompressed one. It also defers the
 * parsing: the returned {@link MaxwellTransactionPayloadEventData} exposes the inner events through
 * a single-pass cursor instead of an eagerly-built list, bounding peak memory for large transactions.
 * <p>
 * Registered for {@link com.github.shyiko.mysql.binlog.event.EventType#TRANSACTION_PAYLOAD} in
 * {@link BinlogConnectorReplicator}. The inner events are surfaced to the replication queue by
 * {@link BinlogConnectorEventListener#onEvent}, which the upstream client does not do.
 */
public class MaxwellTransactionPayloadDeserializer implements EventDataDeserializer<TransactionPayloadEventData> {
	// On-the-wire payload header field types (MySQL's Transaction_payload_event format).
	private static final int OTW_PAYLOAD_HEADER_END_MARK = 0;
	private static final int OTW_PAYLOAD_SIZE_FIELD = 1;
	private static final int OTW_PAYLOAD_COMPRESSION_TYPE_FIELD = 2;
	private static final int OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD = 3;

	// MySQL's transaction payload compression types.
	private static final int COMPRESSION_TYPE_ZSTD = 0;
	private static final int COMPRESSION_TYPE_NONE = 255;

	private final EventDeserializer.CompatibilityMode[] compatibilityModes;

	// Reused across payloads: the static Zstd helpers allocate and free a native ZSTD_DCtx on
	// every call, which is pure overhead at one call per transaction. deserialize() only ever
	// runs on the binlog client's single deserialization thread, so an unsynchronized shared
	// context is safe; it stays allocated for the life of the replicator.
	private final ZstdDecompressCtx zstdContext = new ZstdDecompressCtx();

	public MaxwellTransactionPayloadDeserializer(EventDeserializer.CompatibilityMode... compatibilityModes) {
		this.compatibilityModes = compatibilityModes;
	}

	@Override
	public TransactionPayloadEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
		int payloadSize = 0;
		int compressionType = COMPRESSION_TYPE_NONE;
		int uncompressedSize = 0;

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
					payloadSize = inputStream.readPackedInteger();
					break;
				case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
					compressionType = inputStream.readPackedInteger();
					break;
				case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
					uncompressedSize = inputStream.readPackedInteger();
					break;
				default:
					inputStream.read(fieldLength);
					break;
			}
		}

		if (uncompressedSize == 0) {
			uncompressedSize = payloadSize;
		}

		// The compressed bytes stay in this local only: storing them on the event data would
		// keep them reachable for as long as the event itself.
		byte[] payload = inputStream.read(payloadSize);
		byte[] decompressed;

		switch (compressionType) {
			case COMPRESSION_TYPE_ZSTD:
				decompressed = new byte[uncompressedSize];
				int decompressedBytes;
				try {
					decompressedBytes = zstdContext.decompressByteArray(decompressed, 0, decompressed.length, payload, 0, payload.length);
				} catch (ZstdException e) {
					// zstd-jni reports errors by throwing, not via an error-code return
					throw new IOException("Failed to zstd-decompress binlog transaction payload: " + e.getMessage(), e);
				}
				if (decompressedBytes != uncompressedSize) {
					throw new IOException("Truncated binlog transaction payload: expected "
						+ uncompressedSize + " bytes, zstd produced " + decompressedBytes);
				}
				break;
			case COMPRESSION_TYPE_NONE:
				decompressed = payload;
				break;
			default:
				throw new IOException("Unsupported binlog_transaction_compression type: "
					+ compressionType + " (only ZSTD and NONE are supported)");
		}

		MaxwellTransactionPayloadEventData eventData = new MaxwellTransactionPayloadEventData(decompressed, compatibilityModes);
		eventData.setPayloadSize(payloadSize);
		eventData.setCompressionType(compressionType);
		eventData.setUncompressedSize(uncompressedSize);
		return eventData;
	}
}
