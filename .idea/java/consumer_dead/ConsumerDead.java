package consumer_dead;

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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

import io.vertx.ext.web.RoutingContext;

import org.apache.log4j.PropertyConfigurator;
import org.postgresql.util.PSQLException;
import org.springframework.scheduling.annotation.Scheduled;


public class ConsumerDead {
    private final static String DEAD_QUEUE = "dead_letter_queue";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        try {
            com.rabbitmq.client.Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            handleRequest();

            channel.basicConsume(DEAD_QUEUE, true, (consumerTag, message) -> {
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
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        System.out.println("Не удалось произвести вставку в базу данных, повторная попытка через 5 секунд");
                        handleMessage(content, channel, message);
                    } catch (Exception e) {
                        System.out.println("Ошибка при вызове метода: " + e.getMessage());
                    }
                }
            }, 5000); // Задержка в 5 секунд
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
