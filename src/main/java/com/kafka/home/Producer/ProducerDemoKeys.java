package com.kafka.home.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        //create producer properties
        String bootstrapServer = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i =0 ;i <10; i++) {
            String topic = "first_topic";
            String value = "Kafka Value "+ Integer.toString(i);
            String key = "key_"+ Integer.toString(i);
            ProducerRecord<String, String>  record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key" + key);
            //send Data -- asynchronously
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is successfully sent or an exception is thrown
                    if(e == null) {
                        logger.info("Received new metadata \n" +
                                "Topic" + recordMetadata.topic() + "\n" +
                                "Partition" + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset() + "\n" +
                                "TimesNtamp" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error occurred while producing data");
                    }
                }
            }).get(); //block .send() to make it synchronous -- not recommended in production
        }
        producer.flush();
        producer.close();
    }
}
