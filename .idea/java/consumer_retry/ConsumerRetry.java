package consumer_retry;

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

public class ConsumerRetry {

    private final static String DEAD_QUEUE = "dead_letter_queue";
    private final static String RETRY_QUEUE = "retry_queue";
    private static final String RETRY_EXCHANGE = "retry_exchange";
    private final static int MAX_RETRY_COUNT = 3;

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);

        try {
            com.rabbitmq.client.Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            handleRequest();

            channel.basicConsume(RETRY_QUEUE, true, (consumerTag, message) -> {
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

    /*
   если делать без обменника, то сообщения будут задваиваться и вместо одного сообщения будет 3 сообщения, с разными значениями заголовка
   обменник помогает распознать, что сообщение уже было в очереди и просто меняет значения заголовка (количество попыток отправки)
    */

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
            int retryCount = 0;
            Map<String, Object> headers = new HashMap<>();
            headers.put("retryCount", retryCount); //Мапа для хранения заголовков и их значений
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .headers(headers) // Установить заголовки
                    .build(); //добавление свойства сообщения, в данном случае это добавление заголовков
            while (retryCount < MAX_RETRY_COUNT) {

                System.out.println("Retrying to send message (" + (retryCount + 1) + "): " + content);

                channel.basicPublish(RETRY_EXCHANGE, RETRY_QUEUE, message.getProperties(), message.getBody());
                retryCount++;
                headers.put("retryCount", retryCount); //заново обновление значения заголовка в мапе
                properties = properties.builder().headers(headers).build(); //обновление значения заголовка у сообщения
            }
            if (retryCount >= MAX_RETRY_COUNT) {
                channel.basicPublish("", DEAD_QUEUE, message.getProperties(), message.getBody());
                System.out.println("Maximum retry count reached. Moving message to dead queue: " + content);
            }

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