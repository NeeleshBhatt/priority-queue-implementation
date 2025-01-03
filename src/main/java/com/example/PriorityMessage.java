package com.example;

public class PriorityMessage extends Message implements Comparable<PriorityMessage> {
    private int priority;
    private long createdAt;

    public PriorityMessage() {

    }

    public PriorityMessage(String msgBody, int priority) {
        super(msgBody);
        this.priority = priority;
        this.createdAt = System.currentTimeMillis();
    }

    public PriorityMessage(String msgBody, String receiptId, int priority) {
        super(msgBody, receiptId);
        this.priority = priority;
        this.createdAt = System.currentTimeMillis();
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    @Override
    public int compareTo(PriorityMessage other) {
        // Lower value has higher priority and should come first
        int priorityComparison = Integer.compare(this.priority, other.priority);
        if (priorityComparison != 0) {
            return priorityComparison;
        }

        // For equal priority, using FCFS (earlier createdAt comes first)
        return Long.compare(this.createdAt, other.createdAt);
    }
}


