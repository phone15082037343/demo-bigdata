package com.demo.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException, IOException {
        String filename = "D:\\tools\\data\\1w\\gps_car.csv";
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "server:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        while (true) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>("gps_car",
                        UUID.randomUUID().toString().replaceAll("-", ""),
                        line);
                producer.send(record);
            }
            reader.close();
            Thread.sleep(3000L);
        }

    }

}
