package Database;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.client.model.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MyMongoClient {

    CodecRegistry pojoCodecRegistry;
    MongoDatabase database;

    MongoCollection<RoutingTableEntry> collection;
    String uri = "mongodb+srv://projectebskafka:parola12345@cluster0.iygqz2o.mongodb.net/?retryWrites=true&w=majority";

    public MyMongoClient() {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        connect();
    }

    public void connect() {
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            database = mongoClient.getDatabase("sample_subs").withCodecRegistry(pojoCodecRegistry);
            collection = database.getCollection("subs", RoutingTableEntry.class);
        }
    }

    public List<RoutingTableEntry> getAllEntries(){
        List<RoutingTableEntry> entries = new ArrayList<>();
        collection.find().into(entries);
        return entries;
    }

    public RoutingTableEntry findOneSub(String source) {
        Bson projectionFields = Projections.fields(
                Projections.include("sourceId"),
                Projections.excludeId());
        RoutingTableEntry doc = collection.find(eq("sourceId", source))
                .projection(projectionFields)
                .first();

        return doc;
    }

    public void storeSubscription(String source, String subscriptionJson) {
        RoutingTableEntry entry = findOneSub(source);
        if (entry == null) {
            RoutingTableEntry routingTableEntry = new RoutingTableEntry();
            routingTableEntry.setSourceId(source);
            routingTableEntry.setSubscriptions(new ArrayList<>(Arrays.asList(subscriptionJson)));
            collection.insertOne(routingTableEntry);
        } else {
            Bson query = Filters.eq("sourceId", source);
            Bson update = Updates.push("subscriptions", subscriptionJson);
            UpdateOptions options = new UpdateOptions().upsert(true);
            collection.updateOne(query, update, options);
        }
    }
}
