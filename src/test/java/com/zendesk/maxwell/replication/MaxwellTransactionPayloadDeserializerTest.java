package com.zendesk.maxwell.replication;

import com.github.luben.zstd.Zstd;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MaxwellTransactionPayloadDeserializerTest {
	private static final int COMPRESSION_TYPE_ZSTD = 0;
	private static final int COMPRESSION_TYPE_NONE = 255;
	private static final int XID_EVENT_TYPE_CODE = 16;
	private static final int TRANSACTION_PAYLOAD_TYPE_CODE = 40;

	private static final EventDeserializer.CompatibilityMode[] COMPAT_MODES = {
		EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
		EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
		EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE
	};

	/* a minimal, valid inner event: 19-byte v4 header + 8-byte xid, no checksum */
	static byte[] xidEventBytes(long xid, int timestampSeconds) {
		ByteBuffer buf = ByteBuffer.allocate(27).order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(timestampSeconds);
		buf.put((byte) XID_EVENT_TYPE_CODE);
		buf.putInt(1);             // server id
		buf.putInt(27);            // event length
		buf.putInt(0);             // next position (buffer-relative, re-stamped by the listener)
		buf.putShort((short) 0);   // flags
		buf.putLong(xid);
		return buf.array();
	}

	static byte[] concat(byte[]... arrays) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (byte[] a : arrays)
			out.write(a, 0, a.length);
		return out.toByteArray();
	}

	/* net_field_length encoding, as readPackedInteger expects */
	private static void writePacked(ByteArrayOutputStream out, int value) {
		if (value < 251) {
			out.write(value);
		} else {
			out.write(0xFC);
			out.write(value & 0xFF);
			out.write((value >> 8) & 0xFF);
		}
	}

	private static int packedLength(int value) {
		return value < 251 ? 1 : 3;
	}

	/* the body of a TRANSACTION_PAYLOAD event: (type, length, value) header triplets,
	   end mark, then the payload bytes */
	private static byte[] payloadEventBody(Integer compressionType, Integer uncompressedSize, byte[] payload) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		if (compressionType != null) {
			writePacked(out, 2); // OTW_PAYLOAD_COMPRESSION_TYPE_FIELD
			writePacked(out, packedLength(compressionType));
			writePacked(out, compressionType);
		}
		if (uncompressedSize != null) {
			writePacked(out, 3); // OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD
			writePacked(out, packedLength(uncompressedSize));
			writePacked(out, uncompressedSize);
		}
		writePacked(out, 1); // OTW_PAYLOAD_SIZE_FIELD
		writePacked(out, packedLength(payload.length));
		writePacked(out, payload.length);
		out.write(0); // end mark
		out.write(payload, 0, payload.length);
		return out.toByteArray();
	}

	@Test
	public void testZstdRoundTrip() throws IOException {
		byte[] inner = concat(xidEventBytes(111L, 1000), xidEventBytes(222L, 2000));
		byte[] compressed = Zstd.compress(inner);
		byte[] body = payloadEventBody(COMPRESSION_TYPE_ZSTD, inner.length, compressed);

		MaxwellTransactionPayloadDeserializer deserializer = new MaxwellTransactionPayloadDeserializer(COMPAT_MODES);
		TransactionPayloadEventData data = deserializer.deserialize(new ByteArrayInputStream(body));

		assertThat(data, instanceOf(MaxwellTransactionPayloadEventData.class));
		assertEquals(COMPRESSION_TYPE_ZSTD, data.getCompressionType());
		assertEquals(inner.length, data.getUncompressedSize());
		assertEquals(compressed.length, data.getPayloadSize());
		assertNull("compressed bytes must not be retained", data.getPayload());
		assertTrue("uncompressedEvents stays empty (lazy)", data.getUncompressedEvents().isEmpty());

		MaxwellTransactionPayloadEventData lazy = (MaxwellTransactionPayloadEventData) data;

		Event first = lazy.nextInnerEvent();
		assertEquals(EventType.XID, first.getHeader().getEventType());
		assertEquals(111L, ((XidEventData) first.getData()).getXid());
		assertEquals(1000_000L, first.getHeader().getTimestamp());

		Event second = lazy.nextInnerEvent();
		assertEquals(222L, ((XidEventData) second.getData()).getXid());

		assertNull(lazy.nextInnerEvent());
		assertNull("cursor stays spent after the end", lazy.nextInnerEvent());
	}

	@Test
	public void testFullEventDeserializerIntegration() throws IOException {
		byte[] inner = xidEventBytes(333L, 1500);
		byte[] body = payloadEventBody(COMPRESSION_TYPE_ZSTD, inner.length, Zstd.compress(inner));

		/* a complete on-disk TRANSACTION_PAYLOAD event, parsed through the same outer
		   EventDeserializer wiring BinlogConnectorReplicator sets up */
		ByteBuffer header = ByteBuffer.allocate(19).order(ByteOrder.LITTLE_ENDIAN);
		header.putInt(4242);
		header.put((byte) TRANSACTION_PAYLOAD_TYPE_CODE);
		header.putInt(1);
		header.putInt(19 + body.length);
		header.putInt(98765);
		header.putShort((short) 0);

		EventDeserializer outer = new EventDeserializer();
		outer.setEventDataDeserializer(EventType.TRANSACTION_PAYLOAD, new MaxwellTransactionPayloadDeserializer(COMPAT_MODES));

		Event event = outer.nextEvent(new ByteArrayInputStream(concat(header.array(), body)));
		assertEquals(EventType.TRANSACTION_PAYLOAD, event.getHeader().getEventType());
		assertThat(event.getData(), instanceOf(MaxwellTransactionPayloadEventData.class));

		MaxwellTransactionPayloadEventData data = event.getData();
		assertEquals(333L, ((XidEventData) data.nextInnerEvent().getData()).getXid());
		assertNull(data.nextInnerEvent());
	}

	@Test
	public void testUncompressedPayloadPassthrough() throws IOException {
		byte[] inner = xidEventBytes(444L, 1600);
		/* compression type NONE, no uncompressed-size field: defaults to the payload size */
		byte[] body = payloadEventBody(COMPRESSION_TYPE_NONE, null, inner);

		MaxwellTransactionPayloadDeserializer deserializer = new MaxwellTransactionPayloadDeserializer(COMPAT_MODES);
		TransactionPayloadEventData data = deserializer.deserialize(new ByteArrayInputStream(body));

		assertEquals(COMPRESSION_TYPE_NONE, data.getCompressionType());
		assertEquals(inner.length, data.getUncompressedSize());

		MaxwellTransactionPayloadEventData lazy = (MaxwellTransactionPayloadEventData) data;
		assertEquals(444L, ((XidEventData) lazy.nextInnerEvent().getData()).getXid());
		assertNull(lazy.nextInnerEvent());
	}

	@Test
	public void testCorruptZstdPayloadThrows() {
		byte[] junk = {1, 2, 3, 4, 5, 6, 7, 8};
		byte[] body = payloadEventBody(COMPRESSION_TYPE_ZSTD, 27, junk);

		MaxwellTransactionPayloadDeserializer deserializer = new MaxwellTransactionPayloadDeserializer(COMPAT_MODES);
		try {
			deserializer.deserialize(new ByteArrayInputStream(body));
			fail("expected IOException for a corrupt zstd payload");
		} catch (IOException e) {
			assertThat(e.getMessage(), containsString("zstd"));
		}
	}

	@Test
	public void testTruncatedPayloadThrows() {
		byte[] inner = xidEventBytes(555L, 1700);
		byte[] compressed = Zstd.compress(inner);
		/* declare twice the actual uncompressed size: zstd inflates less than promised */
		byte[] body = payloadEventBody(COMPRESSION_TYPE_ZSTD, inner.length * 2, compressed);

		MaxwellTransactionPayloadDeserializer deserializer = new MaxwellTransactionPayloadDeserializer(COMPAT_MODES);
		try {
			deserializer.deserialize(new ByteArrayInputStream(body));
			fail("expected IOException for a truncated payload");
		} catch (IOException e) {
			assertThat(e.getMessage(), containsString("Truncated"));
		}
	}

	@Test
	public void testUnsupportedCompressionTypeThrows() {
		byte[] body = payloadEventBody(42, 27, new byte[]{1, 2, 3});

		MaxwellTransactionPayloadDeserializer deserializer = new MaxwellTransactionPayloadDeserializer(COMPAT_MODES);
		try {
			deserializer.deserialize(new ByteArrayInputStream(body));
			fail("expected IOException for an unknown compression type");
		} catch (IOException e) {
			assertThat(e.getMessage(), containsString("Unsupported"));
		}
	}
}
