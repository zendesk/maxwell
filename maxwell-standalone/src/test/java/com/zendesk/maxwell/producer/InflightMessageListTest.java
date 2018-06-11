package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by ben on 5/25/16.
 */
@RunWith(MockitoJUnitRunner.class)
public class InflightMessageListTest {
	private static int capacity = 3;
	private static Position p1 = new Position(BinlogPosition.at(1, "f"), 0L);
	private static Position p2 = new Position(BinlogPosition.at(2, "f"), 0L);
	private static Position p3 = new Position(BinlogPosition.at(3, "f"), 0L);
	private static Position p4 = new Position(BinlogPosition.at(4, "f"), 0L);
	private InflightMessageList list;
	private MaxwellContext context;
	@Captor
	private ArgumentCaptor<RuntimeException> captor;

	@Test
	public void testInOrderCompletion() throws InterruptedException {
		setupWithInflightRequestTimeout(0, 0.1);

		Position ret;

		ret = list.completeMessage(p1).position;
		assert(ret.equals(p1));

		ret = list.completeMessage(p2).position;
		assert(ret.equals(p2));

		ret = list.completeMessage(p3).position;
		assert(ret.equals(p3));

		assert(list.size() == 0);
	}

	@Test
	public void testOutOfOrderComplete() throws InterruptedException {
		setupWithInflightRequestTimeout(0, 0.1);

		Position ret;
		InflightMessageList.InflightMessage m;

		m = list.completeMessage(p3);
		assert(m == null);

		m = list.completeMessage(p2);
		assert(m == null);

		ret = list.completeMessage(p1).position;
		assertEquals(p3, ret);
	}

	@Test
	public void testMaxwellWillTerminateWhenHeadOfInflightMsgListIsStuckAndListFullAndMostCompletedAndCheckTurnedOn() throws InterruptedException {
		// Given
		long inflightRequestTimeout = 100;
		setupWithInflightRequestTimeout(inflightRequestTimeout, 0.1);
		list.completeMessage(p2);
		Thread.sleep(inflightRequestTimeout + 5);

		// When
		list.completeMessage(p3);

		// Then
		verify(context).terminate(captor.capture());
		assertThat(captor.getValue().getMessage(), is("Did not receive acknowledgement for the head of the inflight message list for " + inflightRequestTimeout + " ms"));
	}

	@Test
	public void testMaxwellWillNotTerminateWhenHeadOfInflightMsgListIsStuckAndCheckTurnedOff() throws InterruptedException {
		// Given
		setupWithInflightRequestTimeout(0, 0.1);
		list.completeMessage(p2);

		// When
		list.completeMessage(p3);

		// Then
		verify(context, never()).terminate(any(RuntimeException.class));
	}

	@Test
	public void testMaxwellWillNotTerminateWhenHeadOfInflightMsgListIsStuckAndListNotFullAndMostCompletedAndCheckTurnedOn() throws InterruptedException {
		// Given
		long inflightRequestTimeout = 100;
		setupWithInflightRequestTimeout(inflightRequestTimeout, 0.1);
		list.completeMessage(p1);
		Thread.sleep(inflightRequestTimeout + 5);

		// When
		list.completeMessage(p3);

		// Then
		verify(context, never()).terminate(any(RuntimeException.class));
	}

	@Test
	public void testMaxwellWillNotTerminateWhenHeadOfInflightMsgListIsStuckAndListFullAndMostCompletedAndCheckTurnedOn() throws InterruptedException {
		// Given
		long inflightRequestTimeout = 100;
		setupWithInflightRequestTimeout(inflightRequestTimeout, 0.9);
		list.completeMessage(p2);
		Thread.sleep(inflightRequestTimeout + 5);

		// When
		list.completeMessage(p3);

		// Then
		verify(context, never()).terminate(any(RuntimeException.class));
	}

	@Test
	public void testAddMessageWillWaitWhenCapacityIsFull() throws InterruptedException {
		setupWithInflightRequestTimeout(0, 0.1);

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

	private void setupWithInflightRequestTimeout(long timeout, double completePercentageThreshold) throws InterruptedException {
		context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfig();
		config.producerAckTimeout = timeout;
		when(context.getConfig()).thenReturn(config);
		list = new InflightMessageList(context, capacity, completePercentageThreshold);
		list.addMessage(p1);
		list.addMessage(p2);
		list.addMessage(p3);
	}
}
