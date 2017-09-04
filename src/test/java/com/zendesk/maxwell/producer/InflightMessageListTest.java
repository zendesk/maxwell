package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Created by ben on 5/25/16.
 */
public class InflightMessageListTest {
	static int capacity = 3;
	static Position p1 = new Position(BinlogPosition.at(1, "f"), 0L);
	static Position p2 = new Position(BinlogPosition.at(2, "f"), 0L);
	static Position p3 = new Position(BinlogPosition.at(3, "f"), 0L);
	static Position p4 = new Position(BinlogPosition.at(4, "f"), 0L);
	InflightMessageList list;

	@Before
	public void setupBefore() throws InterruptedException {
		list = new InflightMessageList(capacity);
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

	@Test
	public void testAddMessageWillWaitWhenCapacityIsFull() throws InterruptedException {
		AddMessage addMessage = new AddMessage();
		Thread add = new Thread(addMessage);
		add.start();
		assertThat("Should never exceed capacity", list.size(), is(capacity));

		long wait = 500;
		Thread.sleep(wait);
		list.completeMessage(p1);

		add.join();
		assertThat("Should never exceed capacity", list.size(), is(capacity));
		long elapse = addMessage.end - addMessage.start;
		assertThat("Should have waited message to be completed", elapse, greaterThanOrEqualTo(wait));
	}

	class AddMessage implements Runnable {
		long start;
		long end;

		@Override
		public void run() {
			start = System.currentTimeMillis();
			try {
				list.addMessage(p4);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			end = System.currentTimeMillis();
		}
	}
}
