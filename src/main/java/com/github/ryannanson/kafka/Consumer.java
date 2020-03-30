package com.github.ryannanson.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    // Constant address of kafka server.
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    // Constant Group ID
    private static final String GROUP_ID = "new_application";

    // Constant for topic.
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {

        new Consumer().run();
    }

    public Consumer() {

    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(Consumer.class);

        // Latch for dealing with multiple threads.
        CountDownLatch latch = new CountDownLatch(1);

        // Create the consumer runnable.
        logger.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(latch);

        // Start the thread.
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        // Add shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook.");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited.");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing.");
        }
    }

    public static class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;

            // Create Consumer properties.
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Create consumer.
            consumer = new KafkaConsumer<String, String>(properties);

            // Use Arrays.asList to allow subscribe to multiple topics if required.
            consumer.subscribe(Arrays.asList(TOPIC));
        }

        @Override
        public void run() {
            // Poll for new data.
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();

                // Tell our main code we're done with the consumer.
                latch.countDown();
            }
        }

        public void shutdown() {
            // Special method to interrupt consumer.poll() by throwing WakeUpException.
            consumer.wakeup();
        }
    }
}
