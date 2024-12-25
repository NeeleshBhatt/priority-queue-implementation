package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemoryPriorityQueueService implements PriorityQueueService {

    private final Map<String, PriorityBlockingQueue<PriorityMessage>> queues;

    private long visibilityTimeout;

    InMemoryPriorityQueueService() {
        this.queues = new ConcurrentHashMap<>();
        String propFileName = "config.properties";
        Properties confInfo = new Properties();

        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            confInfo.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.visibilityTimeout = Integer.parseInt(confInfo.getProperty("visibilityTimeout", "30"));
    }

    @Override
    public void push(String queueUrl, String msgBody, int priority) {
        PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue == null) {
            queue = new PriorityBlockingQueue<PriorityMessage>();
            queues.put(queueUrl, queue);
        }
        PriorityMessage msg = new PriorityMessage(msgBody, priority);
        queue.add(msg);
    }

    @Override
    public PriorityMessage pull(String queueUrl) {
        PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue == null) {
            return null;
        }

        long nowTime = now();
        List<PriorityMessage> invisibleMessages = new ArrayList<>(); // Temporary storage for invisible messages

        while (!queue.isEmpty()) {
            PriorityMessage msg = queue.poll(); // Retrieve the head element
            if (msg.isVisibleAt(nowTime)) {
                // If the message is visible, update visibility
                msg.setReceiptId(UUID.randomUUID().toString());
                msg.incrementAttempts();
                msg.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));
                queue.add(msg);
                queue.addAll(invisibleMessages); // Reinsert invisible messages
                return new PriorityMessage(msg.getBody(), msg.getReceiptId(), msg.getPriority());
            } else {
                // If the message is not visible, store it temporarily
                invisibleMessages.add(msg);
            }
        }

        // Reinsert all invisible messages if no visible message is found
        queue.addAll(invisibleMessages);
        return null;
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        PriorityBlockingQueue<PriorityMessage> queue = queues.get(queueUrl);
        if (queue != null) {
            long nowTime = now();

            for (PriorityMessage msg : queue) {
                if (!msg.isVisibleAt(nowTime) && msg.getReceiptId().equals(receiptId)) {
                    queue.remove(msg);
                    break;
                }
            }
        }
    }

    long now() {
        return System.currentTimeMillis();
    }
}
