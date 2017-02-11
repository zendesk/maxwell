package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.replication.BinlogPosition;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ben on 5/25/16.
 */
public class InflightMessageListTest {
	static BinlogPosition p1 = BinlogPosition.at(1, "f");
	static BinlogPosition p2 = BinlogPosition.at(2, "f");
	static BinlogPosition p3 = BinlogPosition.at(3, "f");
	InflightMessageList list;

	@Before
	public void setupBefore() {
		list = new InflightMessageList();
		list.addTXMessage(1, p1);
		list.addTXMessage(2, p2);
		list.addTXMessage(3, p3);
	}

	@Test
	public void testInOrderCompletion() {
		BinlogPosition ret;


		ret = list.completeTXMessage(1);
		assert(ret.equals(p1));

		ret = list.completeTXMessage(2);
		assert(ret.equals(p2));

		ret = list.completeTXMessage(3);
		assert(ret.equals(p3));

		assert(list.size() == 0);
	}

	@Test
	public void testOutOfOrderComplete() {
		BinlogPosition ret;

		ret = list.completeTXMessage(3);
		assert(ret == null);

		ret = list.completeTXMessage(2);
		assert(ret == null);

		ret = list.completeTXMessage(1);
		assertEquals(p3, ret);
	}
}
