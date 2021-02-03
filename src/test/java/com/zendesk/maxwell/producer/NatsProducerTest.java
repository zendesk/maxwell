package com.zendesk.maxwell.producer;

import com.google.common.collect.Lists;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import io.nats.client.Connection;
import io.nats.client.Nats;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class NatsProducerTest {

	private static final String TEST_TOPIC = "testDb.testTable.insert";

	private final MaxwellContext contextMock = mock(MaxwellContext.class);

	private final MaxwellConfig config = new MaxwellConfig();

	private final Connection natsConnection = mock(Connection.class);


	@Before
	public void beforeEach() {
		config.natsUrl = "nats://localhost";
		config.natsSubjectPrefix = "";
		config.natsSubjectHierarchies = "%db%.%table%.%type%";

		when(contextMock.getConfig()).thenReturn(config);
		when(contextMock.getMetrics()).thenReturn(new NoOpMetrics());
	}


	@Test()
	public void failToConnectToServer() throws IOException, InterruptedException {
		final IOException ioException = new IOException("Fail to connect to nats server.");
		try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
			theMock.when(() -> Nats.connect(anyString())).thenThrow(
					ioException
			);

			final Throwable thrown = Assert.assertThrows(
					RuntimeException.class,
					() -> new NatsProducer(contextMock)
			);

			Assert.assertEquals(thrown.getCause(), ioException);
		}
	}

	@Test
	public void failInvalidHierarchies() {
		final List<String> invalidPrefixes = Lists.newArrayList(
				"",
				"%db%",
				"%db%.%table%",
				"%db%.%type%",
				"%table%.%type%"
		);

		invalidPrefixes.stream().forEach(s -> {

			config.natsSubjectHierarchies = s;

			try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
				theMock.when(() -> Nats.connect(eq(config.natsUrl))).thenReturn(natsConnection);
				Throwable thrown = Assert.assertThrows(
						IllegalArgumentException.class,
						() -> new NatsProducer(contextMock)
				);

				Assert.assertEquals(String.format("Invalid nats config for subjectHierarchies '%s'", s), thrown.getMessage());
			}
		});

	}

	@Test
	public void pushRowToSubject() throws Exception {

		try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
			theMock.when(() -> Nats.connect(eq(config.natsUrl))).thenReturn(natsConnection);

			NatsProducer natsProducer = new NatsProducer(contextMock);
			RowMap rowMap = newRowMap();

			byte[] expectedBytes = rowMap.toJSON(config.outputConfig).getBytes(StandardCharsets.UTF_8);
			doNothing().when(natsConnection).publish(eq(TEST_TOPIC), eq(expectedBytes));

			natsProducer.push(rowMap);

			verify(natsConnection, times(1)).publish(eq(TEST_TOPIC), eq(expectedBytes));
			verify(contextMock, times(0)).setPosition(any(Position.class));
		}
	}

	@Test
	public void pushRowToSubject_withPrefix() throws Exception {

		config.natsSubjectPrefix = "testPrefix";

		try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
			theMock.when(() -> Nats.connect(eq(config.natsUrl))).thenReturn(natsConnection);

			NatsProducer natsProducer = new NatsProducer(contextMock);
			RowMap rowMap = newRowMap();

			byte[] expectedBytes = rowMap.toJSON(config.outputConfig).getBytes(StandardCharsets.UTF_8);
			doNothing().when(natsConnection).publish(eq("testPrefix.testDb.testTable.insert"), eq(expectedBytes));

			natsProducer.push(rowMap);

			verify(natsConnection, times(1)).publish(eq("testPrefix.testDb.testTable.insert"), eq(expectedBytes));
			verify(contextMock, times(0)).setPosition(any(Position.class));
		}
	}

	@Test
	public void pushRowToSubject_withTxCommit() throws Exception {
		try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
			theMock.when(() -> Nats.connect(eq(config.natsUrl))).thenReturn(natsConnection);

			NatsProducer natsProducer = new NatsProducer(contextMock);
			RowMap rowMap = newRowMap();

			rowMap.setTXCommit();

			byte[] expectedBytes = rowMap.toJSON(config.outputConfig).getBytes(StandardCharsets.UTF_8);
			doNothing().when(natsConnection).publish(eq(TEST_TOPIC), eq(expectedBytes));

			natsProducer.push(rowMap);

			verify(natsConnection, times(1)).publish(eq(TEST_TOPIC), eq(expectedBytes));
			verify(contextMock, times(1)).setPosition(eq(rowMap.getNextPosition()));
		}
	}

	@Test
	public void pushRowToSubject_withoutOutput() throws Exception {

		try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
			theMock.when(() -> Nats.connect(eq(config.natsUrl))).thenReturn(natsConnection);

			NatsProducer natsProducer = new NatsProducer(contextMock);

			RowMap rowMap = newRowMap();
			rowMap.suppress();

			byte[] expectedBytes = rowMap.toJSON(config.outputConfig).getBytes(StandardCharsets.UTF_8);
			doNothing().when(natsConnection).publish(eq(TEST_TOPIC), eq(expectedBytes));

			natsProducer.push(rowMap);

			verify(natsConnection, times(0)).publish(any(), any());
			verify(contextMock, times(1)).setPosition(eq(rowMap.getNextPosition()));
		}
	}

	@Test
	public void failToPushRowToSubject() throws Exception {
		try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
			theMock.when(() -> Nats.connect(eq(config.natsUrl))).thenReturn(natsConnection);

			NatsProducer natsProducer = new NatsProducer(contextMock);
			RowMap rowMap = newRowMap();

			byte[] expectedBytes = rowMap.toJSON(config.outputConfig).getBytes(StandardCharsets.UTF_8);
			RuntimeException exception = new RuntimeException("Failed to publish message.");

			doThrow(exception).when(natsConnection).publish(eq(TEST_TOPIC), eq(expectedBytes));

			Throwable thrown = Assert.assertThrows(RuntimeException.class, () -> {
				natsProducer.push(rowMap);
			});

			Assert.assertEquals(exception, thrown);
			verify(natsConnection, times(1)).publish(eq(TEST_TOPIC), eq(expectedBytes));
			verify(contextMock, times(0)).setPosition(any(Position.class));
		}
	}

	@Test
	public void getSubjectHierarchies() {
		try (MockedStatic<Nats> theMock = Mockito.mockStatic(Nats.class)) {
			theMock.when(() -> Nats.connect(eq(config.natsUrl))).thenReturn(natsConnection);

			NatsProducer natsProducer = new NatsProducer(contextMock);
			Assert.assertEquals(TEST_TOPIC, natsProducer
					.getSubjectHierarchies(newRowMap()));


			Assert.assertEquals(
					"testDb..insert",
					natsProducer.getSubjectHierarchies(new RowMap("insert", "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L)))
			);

			Assert.assertEquals(
					"testDb.testTable.",
					natsProducer.getSubjectHierarchies(new RowMap(null, "testDb", "testTable", System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L)))
			);


			Assert.assertEquals(
					"testDb..",
					natsProducer.getSubjectHierarchies(new RowMap(null, "testDb", null, System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L)))
			);
		}
	}

	private RowMap newRowMap() {
		return new RowMap("insert", "testDb", "testTable", System.currentTimeMillis(), Collections.emptyList(), new Position(new BinlogPosition(3, "mysql.1"), 0L));
	}

}
