package consumer_main;

import com.rabbitmq.client.*;
import io.vertx.core.eventbus.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import io.vertx.ext.web.RoutingContext;

import org.apache.log4j.PropertyConfigurator;
import org.postgresql.util.PSQLException;

public class ConsumerMain {
    private final static String MAIN_QUEUE = "main_queue";
    private final static String RETRY_QUEUE = "retry_queue";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        try {
            com.rabbitmq.client.Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            handleRequest();
            channel.basicConsume(MAIN_QUEUE, true, (consumerTag, message) -> {
                String content = new String(message.getBody(), StandardCharsets.UTF_8);
                try {
                    handleMessage(content, channel, message);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, consumerTag -> {
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    channel.close();
                    connection.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }));
        } catch (IOException | TimeoutException e) {
            System.err.println("Error connecting to RabbitMQ: " + e.getMessage());
        }
    }

    private static void handleMessage(String content, Channel channel, Delivery message) throws Exception {
        try {
            Connection dbConnection = (Connection) Database.connectToDatabase();
            PreparedStatement statement = dbConnection.prepareStatement("INSERT INTO messages (content) VALUES (?)");
            statement.setString(1, content);
            statement.executeUpdate();
            statement.close();
            dbConnection.close();
            channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        } catch (PSQLException e) {
            System.out.println("Error inserting message into the database: " + e.getMessage());
            channel.basicPublish("", RETRY_QUEUE, message.getProperties(), message.getBody());
            System.out.println("Moving message to retry queue: " + content);
        }
    }

    private static void handleRequest() {
        try {
            Connection connection = Database.connectToDatabase();
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, content TEXT)");
            statement.close();
            connection.close();
            System.out.println("Table created successfully");
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
        }
    }
}
