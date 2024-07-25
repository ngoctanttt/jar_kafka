package com.example.kafka.Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaPoCProducer implements Runnable {
    private static final String TOPIC = "test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    @Override
    public void run() {
        produce();
    }
    private void produce() {
        // Create Configuration Options for our Producer and Initialize a New Producer
        Properties props = new Properties();
        System.out.println("Đã chạy vào đây");
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        // We Configure the Serializer to Describe the Format in which we Want To
        // Produce Data into our Kafka Cluster
        props.put("key.serializer", "org.apache.git .common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Since we Need to Close our Producer, We can use the try-with-resources
        // Statement to create a New Producer
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // Here, We Run an Infinite Loop to Send a Message to the Cluster Every Second
            for (int i = 0;; i++) {
                String key = Integer.toString(i);
                String message = "Watson, Please Come Over Here " + Integer.toString(i);
                producer.send(new ProducerRecord<String, String>(TOPIC, key, message));
                // Log a Confirmation Once The Message is Written
                System.out.println("Sent Message " + key);
                try {
                    // Sleep for a Second
                    Thread.sleep(1000);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Could not Start Producer Due To: " + e);
        }
    }
}