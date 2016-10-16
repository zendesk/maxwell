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
		list.addMessage(p1);
		list.addMessage(p2);
		list.addMessage(p3);
	}

	@Test
	public void testInOrderCompletion() {
		BinlogPosition ret;


		ret = list.completeMessage(p1);
		assert(ret.equals(p1));

		ret = list.completeMessage(p2);
		assert(ret.equals(p2));

		ret = list.completeMessage(p3);
		assert(ret.equals(p3));

		assert(list.size() == 0);
	}

	@Test
	public void testOutOfOrderComplete() {
		BinlogPosition ret;

		ret = list.completeMessage(p3);
		assert(ret == null);

		ret = list.completeMessage(p2);
		assert(ret == null);

		ret = list.completeMessage(p1);
		assertEquals(p3, ret);
	}
}