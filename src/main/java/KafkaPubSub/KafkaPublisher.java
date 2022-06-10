package KafkaPubSub;

import Deserializer.MyPublicationListDeserializer;
import Model.Publication;
import Proto.PublicationProtoOuterClass;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import static com.google.protobuf.util.Timestamps.fromMillis;

import java.io.*;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.*;

public class KafkaPublisher {
    public static void main(String[] args) throws IOException, ParseException {

        // Check arguments length value
        if (args.length == 0) {
            System.out.println("Enter broker's topic name");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0];


        List<Publication> publications = loadAllPublications("publications.txt");


        //FOR TESTING
//        List<Publication> publications = new ArrayList<>();
//        publications.add(new Publication("Google", 10.0, new SimpleDateFormat("yyyy-MM-dd").parse("2022-06-05"), 1.39, 0.38));
//        publications.add(new Publication("Apple", 13.0, new SimpleDateFormat("yyyy-MM-dd").parse("2020-06-05"), 3.5, 0.38));
//        publications.add(new Publication("Facebook", 7.3, new SimpleDateFormat("yyyy-MM-dd").parse("2021-06-05"), 3.7, -0.38));
//        publications.add(new Publication("Amazon", 9.0, new SimpleDateFormat("yyyy-MM-dd").parse("2019-06-05"), 2.7, 0.9));

//        sendPublicationsJson(topicName, publications);
        sendPublicationsProto(topicName, publications);
    }

    private static void sendPublicationsJson(String topicName, List<Publication> publications) {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 1);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

//        props.put("enable.idempotence", "true");

//        props.put("transactional.id", "my-transactional-id");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Gson gson = new Gson();
        int pubId = 0;
        int i = 0;
        int pubsSize = publications.size();
        long end = System.currentTimeMillis() + 60000 * 3; //3 min
        try {
            while (System.currentTimeMillis() < end) {
                Publication tmpPublication = publications.get(i);
                String publicationJson = gson.toJson(tmpPublication);
                producer.send(new ProducerRecord<>(topicName,
                        "pub-" + pubId + "-topic", publicationJson));
                pubId++;
                if (pubId % 1000 == 0) {
                    producer.flush();
                }
                i = ++i % pubsSize;
//                System.out.println("Publication sent successfully: " + publicationJson);
            }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
        System.out.println("PUBLICATII TRIMISE: " + pubId);
    }

    private static void sendPublicationsProto(String topicName, List<Publication> publications) {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 1);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

//        props.put("enable.idempotence", "true");

//        props.put("transactional.id", "my-transactional-id");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");

        props.put("schema.registry.url", "http://localhost:8081");

        props.put("auto.register.schemas", "true");

        Producer<String, PublicationProtoOuterClass.PublicationProto> producer = new KafkaProducer<>(props);

        List<PublicationProtoOuterClass.PublicationProto> publicationProtos = new ArrayList<>();
        for(Publication publication: publications){
            PublicationProtoOuterClass.PublicationProto.Builder publicationProtoBuilder =
                    PublicationProtoOuterClass.PublicationProto.newBuilder()
                            .setCompany(publication.getCompany())
                            .setDate(fromMillis(publication.getDate().getTime()))
                            .setDrop(publication.getDrop())
                            .setValue(publication.getValue())
                            .setVariation(publication.getVariation());
            PublicationProtoOuterClass.PublicationProto publicationProto = publicationProtoBuilder.build();
            publicationProtos.add(publicationProto);
        }

        int pubId = 0;
        int i = 0;
        int pubsSize = publications.size();
        long end = System.currentTimeMillis() + 60000 * 3; //3 min
        try {
            while (System.currentTimeMillis() < end) {
                PublicationProtoOuterClass.PublicationProto tmpPublication = publicationProtos.get(i);

                producer.send(new ProducerRecord<>(topicName,
                        "pub-" + pubId + "-topic", tmpPublication));
                pubId++;
                if (pubId % 1000 == 0) {
                    producer.flush();
                }
                i = ++i % pubsSize;
//                System.out.println("Publication sent successfully: " + publicationJson);
            }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
        System.out.println("PUBLICATII TRIMISE: " + pubId);
    }

    private static List<Publication> loadAllPublications(String filename) throws IOException {
        File file = new File(filename);
        BufferedReader br = new BufferedReader(new FileReader(file));
        StringBuilder pubsJson = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            pubsJson.append(line);
        }
        Type collectionType = new TypeToken<List<Publication>>() {
        }.getType();
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(collectionType, new MyPublicationListDeserializer())
                .create();
        List<Publication> publications = gson.fromJson(pubsJson.toString(), collectionType);
        return publications;
    }
}
