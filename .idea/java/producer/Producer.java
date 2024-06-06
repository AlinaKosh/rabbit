package producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String MAIN_QUEUE = "main_queue"; //очеред
    private static final String RETRY_QUEUE = "retry_queue"; //очеред
    private static final String DEAD_LETTER_QUEUE = "dead_letter_queue"; //очеред
    private static final String RETRY_EXCHANGE = "retry_exchange"; // обменник
    private static final String DEAD_LETTER_EXCHANGE = "dead_exchange"; // обменник

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(MAIN_QUEUE, true, false, false, null);
        channel.exchangeDeclare(RETRY_EXCHANGE, "direct", true);
        channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, "direct", true);
        channel.queueDeclare(RETRY_QUEUE, true, false, false, null);
        channel.queueDeclare(DEAD_LETTER_QUEUE, true, false, false, null);
        channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, "direct", true);
        channel.queueBind(RETRY_QUEUE, RETRY_EXCHANGE, "");
        channel.queueBind(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, "");



        List<String> data = DataInform.getData();
        for (int i = 0; i <= data.size(); i++) {
            if (i == data.size()) i = 0;
            String message = data.get(i);
            channel.basicPublish("", MAIN_QUEUE, null, message.getBytes());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
