package com.zendesk.maxwell.replication;

import binlogdata.Binlogdata;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Vitess source position coordiates. */
public class Vgtid {
	public static final String CURRENT_GTID = "current";
	private static final Gson gson = new Gson();

	private final Binlogdata.VGtid rawVgtid;
	private final Set<ShardGtid> shardGtids = new HashSet<>();

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
							.build());
		}
		this.rawVgtid = builder.build();
	}

	public static Vgtid of(String shardGtidsInJson) {
		List<Vgtid.ShardGtid> shardGtids = gson.fromJson(shardGtidsInJson, new TypeToken<List<ShardGtid>>() {
		}.getType());
		return of(shardGtids);
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

	public Set<ShardGtid> getShardGtids() {
		return shardGtids;
	}

	public boolean isSingleShard() {
		return rawVgtid.getShardGtidsCount() == 1;
	}

	@Override
	public String toString() {
		return gson.toJson(shardGtids);
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

	public static class ShardGtid {
		private final String keyspace;
		private final String shard;
		private final String gtid;

		public ShardGtid(String keyspace, String shard, String gtid) {
			this.keyspace = keyspace;
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
