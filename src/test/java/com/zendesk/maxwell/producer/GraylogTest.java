package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.row.RowMapDeserializer;

import org.graylog2.gelfclient.*;
import org.graylog2.gelfclient.transport.GelfTransport;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;

@RunWith(MockitoJUnitRunner.class)
public class GraylogTest {

	private GelfTransport transport;
	private GraylogProdcuer graylogProdcuer;

	@Captor
	private ArgumentCaptor<GelfMessage> captor;

	@Before
	public void setUp() throws Exception {
		MaxwellContext context = mock(MaxwellContext.class);
		transport = mock(GelfTransport.class);

		MaxwellConfig config = new MaxwellConfig();

		Properties additionalField = new Properties();
		additionalField.setProperty("field_foo", "1");
		additionalField.setProperty("field_bar", "2");

		config.graylogAdditionalField = additionalField;
		NoOpMetrics metrics = new NoOpMetrics();
		GelfConfiguration configuration = new GelfConfiguration();
		configuration.transport(GelfTransports.UDP);

		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(metrics);

		graylogProdcuer = new GraylogProdcuer(context, transport);
	}

	@Test
	public void testMessageIsCorrectBuildForInsert() throws Exception {
		RowMap rowMap = RowMapDeserializer.createFromString(
				"{\"database\":\"db\",\"table\":\"test\",\"type\":\"insert\", \"ts\": \"1449786341\", \"data\": { \"id\": \"1\", \"filed\": \"foo\"}}"
		);

		graylogProdcuer.push(rowMap);

		verify(transport, times(2)).trySend(captor.capture());

		List<GelfMessage> messages = captor.getAllValues();

		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.COLUMN), is("id"));
		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.BEFORE), is(""));
		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.AFTER), is("1"));

		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.COLUMN), is("filed"));
		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.BEFORE), is(""));
		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.AFTER), is("foo"));
	}

	@Test
	public void testMessageIsCorrectBuildForUpdate() throws Exception {
		RowMap rowMap = RowMapDeserializer.createFromString(
				"{\"database\":\"db\",\"table\":\"test\",\"type\":\"update\", \"ts\": \"1449786341\", \"data\": { \"id\": \"2\", \"filed\": \"bar\"}, \"old\": { \"id\": \"1\", \"filed\": \"foo\"}}"
		);

		graylogProdcuer.push(rowMap);

		verify(transport, times(2)).trySend(captor.capture());

		List<GelfMessage> messages = captor.getAllValues();

		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.COLUMN), is("id"));
		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.BEFORE), is("1"));
		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.AFTER), is("2"));

		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.COLUMN), is("filed"));
		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.BEFORE), is("foo"));
		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.AFTER), is("bar"));
	}

	@Test
	public void testMessageIsCorrectBuildForDelete() throws Exception {
		RowMap rowMap = RowMapDeserializer.createFromString(
				"{\"database\":\"db\",\"table\":\"test\",\"type\":\"delete\", \"ts\": \"1449786341\", \"data\": { \"id\": \"2\", \"filed\": \"bar\"}}"
		);

		graylogProdcuer.push(rowMap);

		verify(transport, times(2)).trySend(captor.capture());

		List<GelfMessage> messages = captor.getAllValues();

		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.COLUMN), is("id"));
		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.BEFORE), is("2"));
		assertThat(messages.get(0).getAdditionalFields().get(GraylogProdcuer.AFTER), is(""));

		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.COLUMN), is("filed"));
		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.BEFORE), is("bar"));
		assertThat(messages.get(1).getAdditionalFields().get(GraylogProdcuer.AFTER), is(""));
	}

	@Test
	public void testMessageIsCorrectBuildWithAdditionalFieldProperties() throws Exception {
		RowMap rowMap = RowMapDeserializer.createFromString(
				"{\"database\":\"db\",\"table\":\"test\",\"type\":\"delete\", \"ts\": \"1449786341\", \"data\": { \"id\": \"2\"}}"
		);

		graylogProdcuer.push(rowMap);

		verify(transport, times(1)).trySend(captor.capture());

		List<GelfMessage> messages = captor.getAllValues();

		assertThat(messages.get(0).getAdditionalFields().get("field_foo"), is("1"));
		assertThat(messages.get(0).getAdditionalFields().get("field_bar"), is("2"));
	}

	@Test
	public void testMessageIsCorrectBuildWithBasicFields() throws Exception {
		RowMap rowMap = RowMapDeserializer.createFromString(
				"{\"database\":\"db\",\"table\":\"test\",\"type\":\"insert\",\"xid\":\"9\",\"ts\": \"1449786341\", \"data\": { \"id\": \"2\"}}"
		);

		graylogProdcuer.push(rowMap);

		verify(transport, times(1)).trySend(captor.capture());

		List<GelfMessage> messages = captor.getAllValues();

		assertThat(messages.get(0).getAdditionalFields().get("database"), is("db"));
		assertThat(messages.get(0).getAdditionalFields().get("table"), is("test"));
		assertThat(messages.get(0).getAdditionalFields().get("type"), is("insert"));
		assertThat(messages.get(0).getAdditionalFields().get("xid"), is(Long.valueOf("9")));
		assertThat(messages.get(0).getTimestamp(), is(Double.valueOf("1449786341")));
	}
}
