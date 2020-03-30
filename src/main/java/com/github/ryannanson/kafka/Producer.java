package com.github.ryannanson.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    // Address of kafka server.
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {

        // Create Producer properties.
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer.
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i=0; i<10; i++) {
            // Create producer record.
            String topic = "first_topic";
            String value = "Hello world " + i;
            String key = "id_" + i;

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value, key);
            
            LOGGER.info("Key: " + key);

            // Send data.
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // The record was successfully sent.
                        LOGGER.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "TimeStamp: " + recordMetadata.timestamp());
                    } else {
                        // An error occurred while sending record.
                        LOGGER.error("error while producing: ", e);
                    }
                }
            });
        }

        // Flush data.
        producer.flush();

        // Close producer.
        producer.close();
    }
}
