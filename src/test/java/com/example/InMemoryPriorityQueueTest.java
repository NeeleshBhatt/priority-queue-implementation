package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class InMemoryPriorityQueueTest {
	private PriorityQueueService pqs;
	private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";
	
	@Before
	public void setup() {
		pqs = new InMemoryPriorityQueueService();
	}
	
	
	@Test
	public void testSendMessage(){
		pqs.push(queueUrl, "Good message!", 10);
		PriorityMessage msg = pqs.pull(queueUrl);

		assertNotNull(msg);
		assertEquals("Good message!", msg.getBody());
	}
	
	@Test
	public void testPullMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		pqs.push(queueUrl, msgBody, 10);
		Message msg = pqs.pull(queueUrl);

		assertEquals(msgBody, msg.getBody());
		assertTrue(msg.getReceiptId() != null && msg.getReceiptId().length() > 0);
	}

	@Test
	public void testPullEmptyQueue(){
		Message msg = pqs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDoublePull(){
		pqs.push(queueUrl, "Message A.", 10);
		pqs.pull(queueUrl);
		Message msg = pqs.pull(queueUrl);
		assertNull(msg);
	}
	
	@Test
	public void testDeleteMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";
		
		pqs.push(queueUrl, msgBody, 10);
		Message msg = pqs.pull(queueUrl);

		pqs.delete(queueUrl, msg.getReceiptId());
		msg = pqs.pull(queueUrl);
		
		assertNull(msg);
	}

	@Test
	public void testDeleteVisibleMessage(){
		String msgBody = "{ \"name\":\"John\", \"age\":30, \"car\":null }";

		pqs.push(queueUrl, msgBody, 10);
		PriorityMessage msg = pqs.pull(queueUrl);

		pqs.delete(queueUrl, msg.getReceiptId());
		msg = pqs.pull(queueUrl);

		assertNull(msg);
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
		pqs.push(queueUrl, msgStrs[0], 1);
		pqs.push(queueUrl, msgStrs[1], 2);
		pqs.push(queueUrl, msgStrs[2], 3);
		PriorityMessage msg1 = pqs.pull(queueUrl);
		PriorityMessage msg2 = pqs.pull(queueUrl);
		PriorityMessage msg3 = pqs.pull(queueUrl);

		org.junit.Assert.assertTrue(msgStrs[0].equals(msg3.getBody())
				&& msgStrs[1].equals(msg2.getBody()) && msgStrs[2].equals(msg1.getBody()));
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
		pqs.push(queueUrl, msgStrs[0], 10);
		Thread.sleep(100);
		pqs.push(queueUrl, msgStrs[1], 10);
		Thread.sleep(100);
		pqs.push(queueUrl, msgStrs[2], 10);
		PriorityMessage msg1 = pqs.pull(queueUrl);
		PriorityMessage msg2 = pqs.pull(queueUrl);
		PriorityMessage msg3 = pqs.pull(queueUrl);

		org.junit.Assert.assertTrue(msgStrs[0].equals(msg1.getBody())
				&& msgStrs[1].equals(msg2.getBody()) && msgStrs[2].equals(msg3.getBody()));
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
		pqs.push(queueUrl, msgStrs[0], 1);
		Thread.sleep(100);
		pqs.push(queueUrl, msgStrs[1], 1);
		pqs.push(queueUrl, msgStrs[2], 10);
		PriorityMessage msg1 = pqs.pull(queueUrl);
		PriorityMessage msg2 = pqs.pull(queueUrl);
		PriorityMessage msg3 = pqs.pull(queueUrl);

		org.junit.Assert.assertTrue(msgStrs[0].equals(msg2.getBody())
				&& msgStrs[1].equals(msg3.getBody()) && msgStrs[2].equals(msg1.getBody()));
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
		PriorityMessage msg = priorityQueueService.pull(queueUrl);
		assertTrue(msg != null && msg.getBody() == "Message A.");
	}
}
