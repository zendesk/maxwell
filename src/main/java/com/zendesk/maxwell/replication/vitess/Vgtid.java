/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.zendesk.maxwell.replication.vitess;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import binlogdata.Binlogdata;

/** Vitess source position coordinates. */
public class Vgtid {
	public static final String CURRENT_GTID = "current";
	public static final String KEYSPACE_KEY = "keyspace";
	public static final String SHARD_KEY = "shard";
	public static final String GTID_KEY = "gtid";

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final Binlogdata.VGtid rawVgtid;
	private final List<ShardGtid> shardGtids = new ArrayList<>();

	private Vgtid(Binlogdata.VGtid rawVgtid) {
		this.rawVgtid = rawVgtid;
		for (Binlogdata.ShardGtid shardGtid : rawVgtid.getShardGtidsList()) {
			shardGtids.add(new ShardGtid(shardGtid.getKeyspace(), shardGtid.getShard(), shardGtid.getGtid()));
		}
	}

	private Vgtid(List<ShardGtid> shardGtids) {
		this.shardGtids.addAll(shardGtids);

		Binlogdata.VGtid.Builder builder = Binlogdata.VGtid.newBuilder();
		for (ShardGtid shardGtid : shardGtids) {
			builder.addShardGtids(
				Binlogdata.ShardGtid.newBuilder()
					.setKeyspace(shardGtid.getKeyspace())
					.setShard(shardGtid.getShard())
					.setGtid(shardGtid.getGtid())
					.build()
			);
		}
		this.rawVgtid = builder.build();
	}

	public static Vgtid of(String shardGtidsInJson) {
		try {
			List<ShardGtid> shardGtids = MAPPER.readValue(shardGtidsInJson, new TypeReference<List<ShardGtid>>() { });
			return of(shardGtids);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
	}

	public static Vgtid of(Binlogdata.VGtid rawVgtid) {
		return new Vgtid(rawVgtid);
	}

	public static Vgtid of(List<ShardGtid> shardGtids) {
		return new Vgtid(shardGtids);
	}

	public Binlogdata.VGtid getRawVgtid() {
		return rawVgtid;
	}

	public List<ShardGtid> getShardGtids() {
		return shardGtids;
	}

	public boolean isSingleShard() {
		return rawVgtid.getShardGtidsCount() == 1;
	}

	@Override
	public String toString() {
		try {
			return MAPPER.writeValueAsString(shardGtids);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Vgtid vgtid = (Vgtid) o;
		return Objects.equals(rawVgtid, vgtid.rawVgtid) &&
				Objects.equals(shardGtids, vgtid.shardGtids);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rawVgtid, shardGtids);
	}

	@JsonPropertyOrder({ KEYSPACE_KEY, SHARD_KEY, GTID_KEY })
	public static class ShardGtid {
		private final String keyspace;
		private final String shard;
		private final String gtid;

		@JsonCreator
		public ShardGtid(
			@JsonProperty(KEYSPACE_KEY) String keyspace,
			@JsonProperty(SHARD_KEY) String shard,
			@JsonProperty(GTID_KEY) String gtid
		) {
			this.keyspace = keyspace.intern();
			this.shard = shard;
			this.gtid = gtid;
		}

		public String getKeyspace() {
			return keyspace;
		}

		public String getShard() {
			return shard;
		}

		public String getGtid() {
			return gtid;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ShardGtid shardGtid = (ShardGtid) o;
			return Objects.equals(keyspace, shardGtid.keyspace) &&
					Objects.equals(shard, shardGtid.shard) &&
					Objects.equals(gtid, shardGtid.gtid);
		}

		@Override
		public int hashCode() {
			return Objects.hash(keyspace, shard, gtid);
		}
	}
}
