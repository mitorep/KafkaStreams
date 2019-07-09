import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Scanner;


public class Producer {

    public static void main(String[] args) throws Exception {

        final String TOPIC_NAME = "JavaScalaTopic";
        final Logger LOGGER = Logger.getLogger(Producer.class.getName());

        LOGGER.log(Level.INFO, "Kafka Producer running in thread {0}", Thread.currentThread().getName());
        Properties kafkaProps = new Properties();

        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ansible.mito.local:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "0");

        KafkaProducer<String, String> kafkaProducer  = new KafkaProducer<>(kafkaProps);


        ProducerRecord<String, String> record = null;
        try {
            Random rnd = new Random();
            while (true) {

                for (int i = 1; i <= 10; i++) {
                    String key = "machine-" + i;
                    String value = "message" + rnd.nextInt(20); //-- Set value automaticaly

                    //Scanner scan = new Scanner(System.in);  //Set value from console
                    //String value = scan.nextLine();

                    record = new ProducerRecord<>(TOPIC_NAME, key, value);

                    kafkaProducer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata rm, Exception excptn) {
                            if (excptn != null) {
                                LOGGER.log(Level.WARNING, "Error sending message with key {0}\n{1}", new Object[]{key, excptn.getMessage()});
                            } else {
                                LOGGER.log(Level.INFO, "Partition for key-value {0}::{1} is {2}", new Object[]{key, value, rm.partition()});
                            }

                        }
                    });
                    /**
                     * wait before sending next message. this has been done on
                     * purpose
                     */
                    Thread.sleep(1000);
                }

            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Producer thread was interrupted");
        } finally {
            kafkaProducer.close();

            LOGGER.log(Level.INFO, "Producer closed");
        }
    }
}