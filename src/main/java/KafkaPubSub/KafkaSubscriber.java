package KafkaPubSub;

import Model.Subscription;
import Model.SubscriptionEntry;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;

public class KafkaSubscriber {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Enter broker's topic name");
            return;
        }

        String brokerTopic = args[0];
        String topicToListen = "sub-topic-" + UUID.randomUUID();

        List<Subscription> subscriptions = new ArrayList<>();

        Subscription subscription = new Subscription();
        SubscriptionEntry subscriptionEntry = new SubscriptionEntry("COMPANY", "=", "GOOGLE");
        subscription.addSubscriptionEntry(subscriptionEntry);
        subscriptions.add(subscription);

        Producer<String, String> kafkaProducer = getKafkaProducer();
        Gson gson = new Gson();

        for (Subscription tmpSubscription : subscriptions) {
            String subscriptionJson = gson.toJson(tmpSubscription);
            sendSubscription(subscriptionJson, brokerTopic, topicToListen, kafkaProducer);
        }
        kafkaProducer.close();

        //TODO: add threading logic so we don't lose publications while waiting for all subs to be sent
        listenForMatchingPublications(topicToListen);
    }

    private static void listenForMatchingPublications(String topicToListen) {
        //Kafka consumer configuration settings
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "topics");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicToListen));

        //print the topic name
        System.out.println("Subscribed to topic " + topicToListen);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // print the offset,key and value for the consumer records.
                System.out.printf("NEW PUBLICATION RECEIVED: offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
            }
        }
    }

    private static void sendSubscription(String subscriptionJson, String brokerTopic, String topicToListen, Producer<String, String> kafkaProducer) {
        kafkaProducer.send(new ProducerRecord<>(brokerTopic,
                topicToListen, subscriptionJson));
        System.out.println("Subscription sent successfully: " + subscriptionJson);
    }

    private static Producer<String, String> getKafkaProducer() {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}
