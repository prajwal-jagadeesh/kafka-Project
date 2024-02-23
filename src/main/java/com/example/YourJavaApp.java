import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class YourJavaApp {

    private static final String TOPIC_NAME = "test-topic";
    private static final String BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");

    public static void main(String[] args) {
        startProducer();
        startConsumer();
    }

    private static void startProducer() {
        Properties props = KafkaConfig.getProducerProperties();
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            String message = "Message " + i;
            producer.send(new ProducerRecord<>(TOPIC_NAME, Integer.toString(i), message));
            System.out.println("Produced: " + message);
        }

        producer.close();
    }

    private static void startConsumer() {
        Properties props = KafkaConfig.getConsumerProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed record with key=%s, value=%s%n", record.key(), record.value());
            }
        }
    }
}
