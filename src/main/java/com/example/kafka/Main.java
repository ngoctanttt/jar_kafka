package com.example.kafka;

import com.example.kafka.Kafka.KafkaPoCConsumer;
import com.example.kafka.Kafka.KafkaPoCProducer;

public class Main {
    public static void main(String[] args) {
        Thread cThread = new Thread(new KafkaPoCConsumer());
        cThread.start();
        Thread pThread = new Thread(new KafkaPoCProducer());
        pThread.start();
    }
}
