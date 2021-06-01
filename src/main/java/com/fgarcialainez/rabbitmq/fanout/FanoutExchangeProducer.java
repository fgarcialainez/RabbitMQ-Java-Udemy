package com.fgarcialainez.rabbitmq.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FanoutExchangeProducer {

    public static void main(String[] args) {
        // Create connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(FanoutExchangeConfig.USERNAME);
        factory.setPassword(FanoutExchangeConfig.PASSWORD);

        // Open AMQ connection and channel
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Create fanout exchange
            channel.exchangeDeclare(FanoutExchangeConfig.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            // Send a message to the exchange every second
            int count = 1;

            while (true) {
                // Create message
                String message = "Event " + count++;

                // Send the message to the exchange
                channel.basicPublish(FanoutExchangeConfig.EXCHANGE_NAME, "", null, message.getBytes());

                // Log success
                System.out.println("Message published: " + message);

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
