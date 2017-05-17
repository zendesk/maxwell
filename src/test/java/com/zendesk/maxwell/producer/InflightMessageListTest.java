package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by ben on 5/25/16.
 */
public class InflightMessageListTest {
	static Position p1 = new Position(BinlogPosition.at(1, "f"), 0L);
	static Position p2 = new Position(BinlogPosition.at(2, "f"), 0L);
	static Position p3 = new Position(BinlogPosition.at(3, "f"), 0L);
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
		Position ret;


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
		Position ret;

		ret = list.completeMessage(p3);
		assert(ret == null);

		ret = list.completeMessage(p2);
		assert(ret == null);

		ret = list.completeMessage(p1);
		assertEquals(p3, ret);
	}
}
