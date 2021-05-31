package com.fgarcialainez.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SimpleProducer {

    public static void main(String[] args) {
        // Define variables
        String message = "Hello from producer!";

        // Create connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(SimpleConfig.USERNAME);
        factory.setPassword(SimpleConfig.PASSWORD);

        // Open AMQ connection and channel
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Create the queue (if doesn't exists)
            channel.queueDeclare(SimpleConfig.QUEUE_NAME, false, false, false, null);

            // Send the message to the default exchange
            channel.basicPublish("", SimpleConfig.QUEUE_NAME, null, message.getBytes());

            // Log success
            System.out.println("Message published successfully");
            System.out.println("Queue: " + SimpleConfig.QUEUE_NAME);
            System.out.println("Body: " + message);
        }
        catch (IOException | TimeoutException ex) {
            // Log error
            System.out.println("Error publishing message");
            System.out.println(ex.getMessage());
        }
    }
}
