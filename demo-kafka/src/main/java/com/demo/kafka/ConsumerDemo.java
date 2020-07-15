package com.demo.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "server:9092");
        properties.put(CommonClientConfigs.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("gps_car"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            int count = records.count();
            System.out.println("count:" + count);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset());
                System.out.println("key: " + record.key());
                System.out.println("value: " + record.value());
            }
        }

    }

}
