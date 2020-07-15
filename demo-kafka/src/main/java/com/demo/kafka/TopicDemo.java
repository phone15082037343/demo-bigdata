package com.demo.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "server:9092");
        AdminClient client = AdminClient.create(prop);

        createTopic(client);
//        dropTopic(client);
        listTopicNames(client);
    }

    private static void listTopicNames(AdminClient client) throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = client.listTopics();
        KafkaFuture<Set<String>> kafkaFuture = listTopicsResult.names();
        Set<String> names = kafkaFuture.get();
        System.out.println(names);
        client.close();
    }

    private static void createTopic(AdminClient client) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic("gps_car", 5, (short) 1);
        CreateTopicsResult createTopicsResult = client.createTopics(Collections.singletonList(newTopic));
        KafkaFuture<Void> kafkaFuture = createTopicsResult.all();
        Void aVoid = kafkaFuture.get();
        System.out.println(aVoid);
    }

    private static void dropTopic(AdminClient client) throws ExecutionException, InterruptedException {
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singletonList("gps_car"));
        KafkaFuture<Void> kafkaFuture = deleteTopicsResult.all();
        System.out.println(kafkaFuture.get());
    }


}
