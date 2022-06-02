package KafkaPubSub;

import Model.Publication;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaPublisher {
    public static void main(String[] args) {

        // Check arguments length value
        if (args.length == 0) {
            System.out.println("Enter broker's topic name");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0];

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

        Producer<String, String> producer = new KafkaProducer<>(props);

        List<Publication> publications = new ArrayList<>();
        Publication publication1 = new Publication();
        publication1.addPublicationField("COMPANY", "GOOGLE");
        publication1.addPublicationField("VALUE", "10");

        Publication publication2 = new Publication();
        publication2.addPublicationField("COMPANY", "APPLE");
        publication2.addPublicationField("VALUE", "7");

        publications.add(publication1);
        publications.add(publication2);

        Gson gson = new Gson();
        for (Publication tmpPublication : publications) {
            String publicationJson = gson.toJson(tmpPublication);
            producer.send(new ProducerRecord<>(topicName,
                    "pub-topic", publicationJson));
            System.out.println("Publication sent successfully: " + publicationJson);
        }
        producer.close();
    }
}
