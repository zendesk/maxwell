package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.row.RowIdentity;

public interface DestinationBuilder {

	default String buildDestinationString(boolean interpolateChannel, String channel, RowIdentity pk) {
		if (interpolateChannel) {
			return channel
				.replaceAll("%\\{database}", pk.getDatabase())
				.replaceAll("%\\{table}", pk.getTable())
				.replaceAll("%\\{type}", pk.getRowType())
				;
		}
		return channel;
	}
}
