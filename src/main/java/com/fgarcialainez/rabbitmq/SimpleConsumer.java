package com.fgarcialainez.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

public class SimpleConsumer {

    public static void main(String[] args) {
        // Create connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(SimpleConfig.USERNAME);
        factory.setPassword(SimpleConfig.PASSWORD);

        try {
            // Open AMQ connection and channel
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Create the queue (if doesn't exists)
            channel.queueDeclare(SimpleConfig.QUEUE_NAME, false, false, false, null);

            // Subscribe to the queue
            channel.basicConsume(SimpleConfig.QUEUE_NAME,
                    true,
                    (consumerTag, message) -> {
                        String messageBody = new String(message.getBody(), Charset.defaultCharset());

                        // Log success
                        System.out.println("Message consumed successfully");
                        System.out.println("Queue: " + SimpleConfig.QUEUE_NAME);
                        System.out.println("Exchange: " + message.getEnvelope().getExchange());
                        System.out.println("Routing Key: " + message.getEnvelope().getRoutingKey());
                        System.out.println("Delivery Tag: " + message.getEnvelope().getDeliveryTag());
                        System.out.println("Body: " + messageBody);
                    },
                    consumerTag -> {
                        System.out.println("Consumer " + consumerTag + " cancelled");
                    });
        }
        catch (IOException | TimeoutException ex) {
            // Log error
            System.out.println("Error consuming message");
            System.out.println(ex.getMessage());
        }
    }
}
