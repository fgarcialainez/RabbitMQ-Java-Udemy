package com.fgarcialainez.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * Routing key pattern -> country.sport.type
 *
 * Examples:
 *     Tennis Events -> *.tennis.*
 *     Spain Events -> es.*.* or es.#
 *     All Events -> #
 */
public class TopicExchangeConsumer {

    public static void main(String[] args) {
        // Create connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(TopicExchangeConfig.USERNAME);
        factory.setPassword(TopicExchangeConfig.PASSWORD);

        try {
            // Open AMQ connection and channel
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Create topic exchange
            channel.exchangeDeclare(TopicExchangeConfig.EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            // Prompt the user to enter the routing-key
            System.out.println("Please enter the routing-key: ");
            Scanner scanner = new Scanner(System.in);
            String routingKey = scanner.nextLine();

            // Create the queue and associate to the exchange
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, TopicExchangeConfig.EXCHANGE_NAME, routingKey);

            // Subscribe to the queue
            channel.basicConsume(queueName,
                    true,
                    (consumerTag, message) -> {
                        String messageBody = new String(message.getBody(), Charset.defaultCharset());

                        // Log received message and routing key
                        System.out.println();
                        System.out.println("Message received: " + messageBody);
                        System.out.println("Routing key: " + message.getEnvelope().getRoutingKey());
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
