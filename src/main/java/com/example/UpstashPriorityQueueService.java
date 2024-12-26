package com.example;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class UpstashPriorityQueueService implements PriorityQueueService {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String redisUrl;
    private final String redisToken;
    private final long visibilityTimeout;

    public UpstashPriorityQueueService() {
        String propFileName = "config.properties";
        Properties confInfo = new Properties();

        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            confInfo.load(inStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }

        this.redisUrl = confInfo.getProperty("upstashRedisUrl");
        this.redisToken = confInfo.getProperty("upstashRedisToken");
        this.visibilityTimeout = Long.parseLong(confInfo.getProperty("visibilityTimeout", "30"));
    }

    @Override
    public void push(String queueUrl, String msgBody, int priority) {
        PriorityMessage msg = new PriorityMessage(msgBody, priority);
        try {
            String message = OBJECT_MAPPER.writeValueAsString(msg);
            double score = priority + (msg.getCreatedAt() / 1e13); // Priority as main score, timestamp for FCFS.

            executeRedisCommand("ZADD", queueUrl, String.valueOf(score), message);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to push message to queue", e);
        }
    }

    @Override
    public Message pull(String queueUrl) {
        try {
            long nowTime = now();

            // Fetch all the messages sorted by priority and timestamp
            String response = executeRedisCommand("ZRANGE", queueUrl, "0", "-1");
            if (response == null || response.isEmpty() || response.equals("[]")) {
                return null;
            }

            List<PriorityMessage> messages = parseMessages(response);

            for (PriorityMessage message : messages) {
                if (message.isVisibleAt(nowTime)) {
                    executeRedisCommand("ZREM", queueUrl, OBJECT_MAPPER.writeValueAsString(message)); // Remove old message

                    message.setReceiptId(UUID.randomUUID().toString());
                    message.incrementAttempts();
                    message.setVisibleFrom(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(visibilityTimeout));
                    double score = message.getPriority() + (message.getCreatedAt() / 1e13);
                    executeRedisCommand("ZADD", queueUrl, String.valueOf(score), OBJECT_MAPPER.writeValueAsString(message)); // Add updated message
                    return new Message(message.getBody(), message.getReceiptId());
                }
            }

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to pull message from queue", e);
        }
        return null;
    }

    @Override
    public void delete(String queueUrl, String receiptId) {

        try {
            String response = executeRedisCommand("ZRANGE", queueUrl, "0", "-1");
            if (response != null && !response.isEmpty() && !response.equals("[]")) {
                List<PriorityMessage> allMessages = parseMessages(response);
                long nowTime = now();

                for (PriorityMessage msg : allMessages) {
                    if (!msg.isVisibleAt(nowTime) && msg.getReceiptId().equals(receiptId)) {
                        executeRedisCommand("ZREM", queueUrl, OBJECT_MAPPER.writeValueAsString(msg));
                        break;
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to delete message from queue", e);
        }
    }

    private String executeRedisCommand(String... command) throws IOException, InterruptedException {
        String requestBody = OBJECT_MAPPER.writeValueAsString(command);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(redisUrl))
                .header("Authorization", "Bearer " + redisToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Redis command failed: " + response.body());
        }
        return OBJECT_MAPPER.readTree(response.body()).get("result").toString();
    }

    private List<PriorityMessage> parseMessages(String response) throws IOException {
        String[] messageArray = OBJECT_MAPPER.readValue(response, String[].class);
        List<PriorityMessage> messages = new ArrayList<>();
        for (String message : messageArray) {
            messages.add(OBJECT_MAPPER.readValue(message, PriorityMessage.class));
        }
        return messages;
    }

    long now() {
        return System.currentTimeMillis();
    }
}


