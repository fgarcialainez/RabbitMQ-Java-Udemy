package com.fgarcialainez.rabbitmq.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

public class FanoutExchangeConsumer {

    public static void main(String[] args) {
        // Create connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(FanoutExchangeConfig.USERNAME);
        factory.setPassword(FanoutExchangeConfig.PASSWORD);

        try {
            // Open AMQ connection and channel
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Create fanout exchange
            channel.exchangeDeclare(FanoutExchangeConfig.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            // Create the queue and associate to the exchange
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, FanoutExchangeConfig.EXCHANGE_NAME, "");

            // Subscribe to the queue
            channel.basicConsume(queueName,
                    true,
                    (consumerTag, message) -> {
                        String messageBody = new String(message.getBody(), Charset.defaultCharset());

                        // Log success
                        System.out.println("Message received: " + messageBody);
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
