package com.kafka.home.Consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId= "myIntellijApplication3";
        CountDownLatch latch = new CountDownLatch(1);
        String topic = "first_topic";
        logger.info("Creating consumer thread");
        ConsumerThread consumerThread = new ConsumerThread(groupId, bootstrapServer, topic, latch);

        Thread consumerRunnableThread = new Thread(consumerThread);
        consumerRunnableThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            consumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application Interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }
    private class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        Properties properties = new Properties();

        ConsumerThread(String groupId, String bootstrapServer, String topic, CountDownLatch latch) {
            this.latch = latch;
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: "+ record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch(WakeupException e) {
                System.out.print("Received Shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}