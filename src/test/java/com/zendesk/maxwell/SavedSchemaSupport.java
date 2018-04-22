package com.zendesk.maxwell;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.MysqlSavedSchema;
import com.zendesk.maxwell.schema.Schema;

final public class SavedSchemaSupport {
	private SavedSchemaSupport() { }

	public static MysqlSavedSchema getSavedSchema(MaxwellContext context, Schema schema, Position position) throws Exception {
		if (context.getConfig().gtidMode) {
			return new MysqlSavedSchema(context, schema, position);
		}

		return new MysqlSavedSchema(context.getServerID(), context.getCaseSensitivity(),
				schema, new Position(new BinlogPosition(position.getBinlogPosition().getOffset() - 1L,
				position.getBinlogPosition().getFile()), position.getLastHeartbeatRead()));
	}
}
