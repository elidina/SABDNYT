package producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutionException;

public class App {

    public static void runProducer(String row, int key, String topic){
        Producer<String, String> producer = ProducerCreator.createProducer();

        try {
            sendMessage(producer, key + "", row, topic);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        producer.flush();
        producer.close();
    }


    /**
     * Invia un messaggio legato ad un certo topic
     * @param producer
     * @param key
     * @param value
     * @param topic
     */
    private static void sendMessage(Producer<String, String> producer, String key, String value, String topic){
        try {
            producer.send(
                    new ProducerRecord<String, String>(topic, key, value))
                    .get();
            System.out.println("Sent message: (" + key + ", " + value + ")");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
