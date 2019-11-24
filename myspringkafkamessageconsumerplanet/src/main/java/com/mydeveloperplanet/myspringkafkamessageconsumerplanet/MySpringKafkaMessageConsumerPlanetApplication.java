package com.mydeveloperplanet.myspringkafkamessageconsumerplanet;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class MySpringKafkaMessageConsumerPlanetApplication {

    private static final String TOPIC_NAME = "my-spring-kafka-message-topic";

    public static void main(String[] args) {
        SpringApplication.run(MySpringKafkaMessageConsumerPlanetApplication.class, args);
    }

    @KafkaListener(groupId = "mykafkagroup", topics = TOPIC_NAME) //, properties = { "enable.auto.commit=true", "auto.commit.interval.ms=1000", "poll-interval=100"})
    public void listen(ConsumerRecord<?, ?> record) {
        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
    }

}
