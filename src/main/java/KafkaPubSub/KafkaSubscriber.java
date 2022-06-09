package KafkaPubSub;

import Deserializer.MySubscriptionListDeserializer;
import Model.Subscription;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaSubscriber {
    public static void main(String[] args) throws IOException, ParseException {
        if (args.length == 0) {
            System.out.println("Enter broker's topic name");
            return;
        }

        String brokerTopic = args[0];
        String topicToListen = "sub-topic-" + UUID.randomUUID();
        List<Subscription> subscriptions = loadAllSubscriptions("subscriptions_random.txt");

//        //FOR TESTING
        //1 MATCH
//        List<Subscription> subscriptions = new ArrayList<>();
//        Subscription subscription1 = new Subscription();
//        subscription1.setCompany("Google");
//        subscription1.setValue(10.0);
//        Map<String, String> fieldOperator = Stream.of(new String[][]{
//                {"Company", "="},
//                {"Value", ">="},
//        }).collect(Collectors.toMap(data -> data[0], data -> data[1]));
//        subscription1.setFieldOperator(fieldOperator);
//        subscriptions.add(subscription1);
//        //2 MATCHES
//        Subscription subscription2 = new Subscription();
//        subscription2.setDate(new SimpleDateFormat("yyyy-MM-dd").parse("2011-06-05"));
//        Map<String, String> fieldOperator2 = Stream.of(new String[][]{
//                {"Date", ">"}
//        }).collect(Collectors.toMap(data -> data[0], data -> data[1]));
//        subscription2.setFieldOperator(fieldOperator2);
//        subscriptions.add(subscription2);

        Producer<String, String> kafkaProducer = getKafkaProducer();
        Gson gson = new Gson();
        for (Subscription tmpSubscription : subscriptions) {
            String subscriptionJson = gson.toJson(tmpSubscription);
            sendSubscription(subscriptionJson, brokerTopic, topicToListen, kafkaProducer);
        }
        kafkaProducer.close();

        listenForMatchingPublications(topicToListen);
    }

    private static List<Subscription> loadAllSubscriptions(String filename) throws IOException {
        File file = new File(filename);
        BufferedReader br = new BufferedReader(new FileReader(file));
        StringBuilder subsJson = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            subsJson.append(line);
        }
        Type collectionType = new TypeToken<List<Subscription>>() {
        }.getType();
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(collectionType, new MySubscriptionListDeserializer())
                .create();
        List<Subscription> subscriptions = gson.fromJson(subsJson.toString(), collectionType);
        return subscriptions;
    }

    private static void listenForMatchingPublications(String topicToListen) {
        //Kafka consumer configuration settings
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "topic-consumers");// + "-" + UUID.randomUUID());
        props.put("enable.auto.commit", "false");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "30000");
//        props.put("max.poll.interval.ms", "10000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicToListen));

        //print the topic name
        System.out.println("Subscribed to topic " + topicToListen);
        int pubsReceived = 0;
        long totalTime = 0;
        double meanTime = 0;
        long end = System.currentTimeMillis() + 60000 * 10; // 10 minute timer to prepare the env
        boolean startMeasuring = false;
        while (System.currentTimeMillis() < end) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                if (!startMeasuring) {
                    startMeasuring = true;
                    end = System.currentTimeMillis() + 60000 * 3; // 3 minutes from now
                }
                pubsReceived++;
                long elapsedTime = System.currentTimeMillis() - record.timestamp();
                totalTime += elapsedTime;
                System.out.printf("NEW PUBLICATION RECEIVED: offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
            }
        }
        meanTime = totalTime / (double) pubsReceived;
        System.out.println("NR DE PUBLICATII PRIMITE: " + pubsReceived);
        System.out.println("LATENTA MEDIE: " + meanTime);
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
