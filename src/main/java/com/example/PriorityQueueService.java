package com.example;

public interface PriorityQueueService {
  /** push a message onto a queue. */
  public void push(String queueUrl, String msgBody, int priority);

  /** retrieves a single message from a queue. */
  public Message pull(String queueUrl);

  /** deletes a message from the queue that was received by pull(). */
  public void delete(String queueUrl, String receiptId);
}
