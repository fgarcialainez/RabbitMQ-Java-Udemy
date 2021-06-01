package com.fgarcialainez.rabbitmq.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TopicExchangeProducer {

    public static void main(String[] args) {
        // Create connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(TopicExchangeConfig.USERNAME);
        factory.setPassword(TopicExchangeConfig.PASSWORD);

        // Open AMQ connection and channel
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Create topic exchange
            channel.exchangeDeclare(TopicExchangeConfig.EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            // Create topics lists
            List<String> countries = Arrays.asList("es", "fr", "usa");
            List<String> sports = Arrays.asList("football", "tennis", "skiing");
            List<String> types = Arrays.asList("live", "news");

            // Send a message to the exchange every second
            int count = 1;

            while (true) {
                // Shuffle the topics
                Collections.shuffle(countries);
                Collections.shuffle(sports);
                Collections.shuffle(types);

                // Get the first topic for each list
                String country = countries.get(0);
                String sport = sports.get(0);
                String type = types.get(0);

                // Generate the routing key (country.sport.type)
                String routingKey = country + "." + sport + "." + type;

                // Create message
                String message = "Event " + count++;

                // Send the message to the exchange
                channel.basicPublish(TopicExchangeConfig.EXCHANGE_NAME, routingKey, null, message.getBytes());

                // Log message and routing key
                System.out.println("Message published (" + routingKey + "): " + message);

                // Sleep 2 seconds
                Thread.sleep(2000);
            }
        }
        catch (IOException | TimeoutException | InterruptedException ex) {
            // Log error
            System.out.println("Error publishing message");
            System.out.println(ex.getMessage());
        }
    }
}
