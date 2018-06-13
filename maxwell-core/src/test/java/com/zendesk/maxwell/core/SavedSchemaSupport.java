package com.zendesk.maxwell.core;

import com.zendesk.maxwell.core.replication.BinlogPosition;
import com.zendesk.maxwell.core.replication.Position;
import com.zendesk.maxwell.core.schema.MysqlSavedSchema;
import com.zendesk.maxwell.core.schema.Schema;

final public class SavedSchemaSupport {
	private SavedSchemaSupport() { }

	public static MysqlSavedSchema getSavedSchema(MaxwellSystemContext context, Schema schema, Position position) throws Exception {
		if (context.getConfig().getGtidMode()) {
			return new MysqlSavedSchema(context, schema, position);
		}

		return new MysqlSavedSchema(context.getServerID(), context.getCaseSensitivity(),
				schema, new Position(new BinlogPosition(position.getBinlogPosition().getOffset() - 1L,
				position.getBinlogPosition().getFile()), position.getLastHeartbeatRead()));
	}
}
