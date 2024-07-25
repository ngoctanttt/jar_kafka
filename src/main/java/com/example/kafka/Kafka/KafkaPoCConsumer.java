package com.example.kafka.Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaPoCConsumer implements Runnable {
    private static final String TOPIC = "test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    @Override
    public void run() {
        consume();
    }
    private void consume() {
        // Create Configuration Options for our Consumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        System.out.println("Đã chạy vào đây nữa");
        // The Group ID is a Unique Identified for Each Consumer Group
        props.setProperty("group.id", "my-group-id");
        // Since our Producer uses a String Serializer, We need to use the Corresponding
        // Deserializer
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // Every Time We Consume a Message from kafka, We Need to "commit", That Is,
        // Acknowledge Receipts of the Messages.... We Can Set up an Auto-Commit at
        // Regular intervals, so that this is Taken Care of in the Background
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        // Since We Need to Close our Consumer, We can Use the try-with-resources
        // Statement to Create It
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Subscribe this Consumer to the Same Topic that we Wrote Messages to Earlier
            consumer.subscribe(Arrays.asList(TOPIC));
            // Run an Infinite Loop where we Consume and Print New Messages to the Topic
            while (true) {
                // The consumer.poll Method Checks and Waits..For Any New Messages To Arrive For
                // The Subscribed Topic in case there are No Messages for the Duration Specified
                // In the Argument (1000 ms In this Case), It returns an Empty List
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received Message: %s\n", record.value());
                }
            }
        }
    }
}