package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class UpstashPriorityQueueTest {
	private PriorityQueueService upqs;
	private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";
	
	@Before
	public void setup() {
		upqs = new UpstashPriorityQueueService();
	}
	
	
	@Test
	public void testSendMessage(){
		upqs.push(queueUrl, "Good message!", 10);
		Message msg = upqs.pull(queueUrl);

		assertNotNull(msg);
		assertEquals("Good message!", msg.getBody());

		upqs.delete(queueUrl, msg.getReceiptId());
	}
	
	@Test
	public void testPullMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		upqs.push(queueUrl, msgBody, 10);
		Message msg = upqs.pull(queueUrl);

		assertEquals(msgBody, msg.getBody());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);

		upqs.delete(queueUrl, msg.getReceiptId());
	}

	@Test
	public void testPullEmptyQueue(){
		Message msg = upqs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDoublePull(){
		upqs.push(queueUrl, "Message A.", 10);
		Message msg1 = upqs.pull(queueUrl);
		Message msg2 = upqs.pull(queueUrl);
		assertNull(msg2);

		upqs.delete(queueUrl, msg1.getReceiptId());
	}
	
	@Test
	public void testDeleteMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		upqs.push(queueUrl, msgBody, 10);
		Message msg = upqs.pull(queueUrl);

		upqs.delete(queueUrl, msg.getReceiptId());
		msg = upqs.pull(queueUrl);
		
		assertNull(msg);
	}

	@Test
	public void testDeleteVisibleMessage(){
		UpstashPriorityQueueService upstashPriorityQueueService = new UpstashPriorityQueueService() {
			long now() {
				return System.currentTimeMillis() + 1000 * 30 + 1;
			}
		};

		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";

		upstashPriorityQueueService.push(queueUrl, msgBody, 10);
		Message msg = upstashPriorityQueueService.pull(queueUrl);

		upstashPriorityQueueService.delete(queueUrl, msg.getReceiptId());
		msg = upstashPriorityQueueService.pull(queueUrl);

		assertNotNull(msg);

		upqs.delete(queueUrl, msg.getReceiptId());
	}

	@Test
	public void testPriority3Msgs(){
		String [] msgStrs = {"TEst msg 1", "test msg 2",
				"{\n" + 								// test with multi-line message.
						"    \"name\":\"John\",\n" +
						"    \"age\":30,\n" +
						"    \"cars\": {\n" +
						"        \"car1\":\"Ford\",\n" +
						"        \"car2\":\"BMW\",\n" +
						"        \"car3\":\"Fiat\"\n" +
						"    }\n" +
						" }"};
		upqs.push(queueUrl, msgStrs[0], 1);
		upqs.push(queueUrl, msgStrs[1], 2);
		upqs.push(queueUrl, msgStrs[2], 3);
		Message msg1 = upqs.pull(queueUrl);
		Message msg2 = upqs.pull(queueUrl);
		Message msg3 = upqs.pull(queueUrl);

		org.junit.Assert.assertTrue(msgStrs[0].equals(msg1.getBody())
				&& msgStrs[1].equals(msg2.getBody()) && msgStrs[2].equals(msg3.getBody()));

		upqs.delete(queueUrl, msg1.getReceiptId());
		upqs.delete(queueUrl, msg2.getReceiptId());
		upqs.delete(queueUrl, msg3.getReceiptId());
	}

	@Test
	public void testFCFS3Msgs() throws InterruptedException {
		String [] msgStrs = {"TEst msg 1", "test msg 2",
				"{\n" + 								// test with multi-line message.
						"    \"name\":\"John\",\n" +
						"    \"age\":30,\n" +
						"    \"cars\": {\n" +
						"        \"car1\":\"Ford\",\n" +
						"        \"car2\":\"BMW\",\n" +
						"        \"car3\":\"Fiat\"\n" +
						"    }\n" +
						" }"};
		upqs.push(queueUrl, msgStrs[0], 10);
		Thread.sleep(100);
		upqs.push(queueUrl, msgStrs[1], 10);
		Thread.sleep(100);
		upqs.push(queueUrl, msgStrs[2], 10);
		Message msg1 = upqs.pull(queueUrl);
		Message msg2 = upqs.pull(queueUrl);
		Message msg3 = upqs.pull(queueUrl);

		org.junit.Assert.assertTrue(msgStrs[0].equals(msg1.getBody())
				&& msgStrs[1].equals(msg2.getBody()) && msgStrs[2].equals(msg3.getBody()));

		upqs.delete(queueUrl, msg1.getReceiptId());
		upqs.delete(queueUrl, msg2.getReceiptId());
		upqs.delete(queueUrl, msg3.getReceiptId());
	}

	@Test
	public void testPriorityAndFCFS3Msgs() throws InterruptedException {
		String [] msgStrs = {"TEst msg 1", "test msg 2",
				"{\n" + 								// test with multi-line message.
						"    \"name\":\"John\",\n" +
						"    \"age\":30,\n" +
						"    \"cars\": {\n" +
						"        \"car1\":\"Ford\",\n" +
						"        \"car2\":\"BMW\",\n" +
						"        \"car3\":\"Fiat\"\n" +
						"    }\n" +
						" }"};
		upqs.push(queueUrl, msgStrs[0], 1);
		Thread.sleep(100);
		upqs.push(queueUrl, msgStrs[1], 1);
		upqs.push(queueUrl, msgStrs[2], 10);
		Message msg1 = upqs.pull(queueUrl);
		Message msg2 = upqs.pull(queueUrl);
		Message msg3 = upqs.pull(queueUrl);

		org.junit.Assert.assertTrue(msgStrs[0].equals(msg1.getBody())
				&& msgStrs[1].equals(msg2.getBody()) && msgStrs[2].equals(msg3.getBody()));

		upqs.delete(queueUrl, msg1.getReceiptId());
		upqs.delete(queueUrl, msg2.getReceiptId());
		upqs.delete(queueUrl, msg3.getReceiptId());
	}
	
	@Test
	public void testAckTimeout(){
		InMemoryPriorityQueueService priorityQueueService = new InMemoryPriorityQueueService() {
			long now() {
				return System.currentTimeMillis() + 1000 * 30 + 1;
			}
		};

		priorityQueueService.push(queueUrl, "Message A.", 10);
		priorityQueueService.pull(queueUrl);
		Message msg = priorityQueueService.pull(queueUrl);
		assertTrue(msg != null && msg.getBody() == "Message A.");

		upqs.delete(queueUrl, msg.getReceiptId());
	}
}
