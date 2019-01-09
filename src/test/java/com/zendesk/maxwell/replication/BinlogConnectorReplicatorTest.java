package com.zendesk.maxwell.replication;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellTestSupport;
import com.zendesk.maxwell.MysqlIsolatedServer;
import com.zendesk.maxwell.TestWithNameLogging;
import com.zendesk.maxwell.bootstrap.SynchronousBootstrapper;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.producer.BufferedProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class BinlogConnectorReplicatorTest extends TestWithNameLogging {

	private class DisconnectingDeserializer	extends WriteRowsEventDataDeserializer {
		public DisconnectingDeserializer(Map<Long, TableMapEventData> tableMapEventByTableId) {
			super(tableMapEventByTableId);
		}

		@Override
		public WriteRowsEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
			WriteRowsEventData d = super.deserialize(inputStream);
			System.out.println(d.getRows());
			//if ( d.getRows().get(0).toString().equals("hello"))
			return d;
		}
	}

	@Test
	public void testGTIDReconnects() throws Exception {
		MysqlIsolatedServer server = MaxwellTestSupport.setupServer("--gtid_mode=ON --enforce-gtid-consistency=true");
		MaxwellTestSupport.setupSchema(server);

		server.execute("create table test.t ( i int )");
		server.execute("insert into test.t set i = 1");

		Position position = Position.capture(server.getConnection(), true);
		MaxwellContext context = MaxwellTestSupport.buildContext(server.getPort(), position, null);

		BufferedProducer producer = new BufferedProducer(context, 1);
		NoOpMetrics metrics = new NoOpMetrics();


		BinlogConnectorReplicator replicator = new BinlogConnectorReplicator(
			new MysqlSchemaStore(context, position),
			producer,
			new SynchronousBootstrapper(context),
			context.getConfig().maxwellMysql,
			333098L,
			"maxwell",
			metrics,
			position,
			false,
			"maxwell-client",
			new HeartbeatNotifier(),
			null,
			context.getFilter()
		);

		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(
			EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
			EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
			EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE
		);
		eventDeserializer.setEventDataDeserializer(EventType.EXT_WRITE_ROWS, new DisconnectingDeserialzier());

		replicator.client.setEventDeserializer();

		replicator.startReplicator();

		server.execute("insert into test.t set i = 2");
		RowMap r = replicator.getRow();
	}
}
