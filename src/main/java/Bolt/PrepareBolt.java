package Bolt;

import Model.Publication;
import Model.Subscription;
import com.google.gson.Gson;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.stream.Collectors;

public class PrepareBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Gson gson;

    private Map<String, List<Subscription>> routingTable;
    private Map<String, List<String>> neighborsTopicList;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.gson = new Gson();
        routingTable = new HashMap<>();
        neighborsTopicList = new HashMap<>();
        neighborsTopicList.put("broker-topic-1", new ArrayList<>(Arrays.asList("broker-topic-2")));
        neighborsTopicList.put("broker-topic-2", new ArrayList<>(Arrays.asList("broker-topic-1")));
    }

    @Override
    public void execute(Tuple tuple) {
        String responseTopic = tuple.getStringByField("key");
        if (responseTopic == null) {
            return;
        }
        String payload = tuple.getStringByField("value");
        String currentTopic = tuple.getStringByField("topic");
        if (responseTopic.startsWith("pub")) {
            Publication publication = gson.fromJson(payload, Publication.class);
            System.out.println("[BROKER]PUBLICATION RECEIVED: " + responseTopic + " value: " + publication);
            List<String> destinations = new ArrayList<>();
            for (Map.Entry<String, List<Subscription>> entry : routingTable.entrySet()) {
                for (Subscription subscription : entry.getValue()) {
                    //TODO: CHECK FOR MATCHING SUBSCRIPTIONS
                    //Not the best algo => if we have 2 equal subscriptions it will send the publication twice
                    //But in our case we don't have 2 equal subscriptions :)

                    //IF MATCH => ADD TOPIC TO DESTINATIONS
                    destinations.add(entry.getKey());
                    break;
                }
            }
            for (String destinationTopic : destinations) {
                this.outputCollector.emit(new Values(destinationTopic, "pub-topic", payload));
                System.out.println("[BROKER]PUBLICATION SENT: "+ currentTopic + " -> " + destinationTopic);
            }
        } else {
            //SUBSCRIPTION RECEIVED FROM BROKER/SUB (WE TREAT THEM THE SAME)
            Subscription subscription = gson.fromJson(payload, Subscription.class);
            System.out.println("[BROKER]SUBSCRIPTION RECEIVED listening response on topic: " + responseTopic + " value: " + subscription);
            //Add (subscriber/broker topic, message) to routing table
            List<Subscription> existingSubs = routingTable.get(responseTopic);
            if (existingSubs == null) {
                existingSubs = new ArrayList<>();
                routingTable.put(responseTopic, existingSubs);
            }
            existingSubs.add(subscription);
            // Foreach neighbor broker emit tuple with the (key, message)
            // key = source = current broker
            List<String> neighborsBrokers = neighborsTopicList.get(currentTopic);
            for (String neighborBrokerTopic : neighborsBrokers) {
                if (!responseTopic.equals(neighborBrokerTopic)) {
                    this.outputCollector.emit(new Values(neighborBrokerTopic, currentTopic, payload));
                    System.out.println("[BROKER]SUBSCRIPTION SENT: " + currentTopic + " -> " + neighborBrokerTopic + " value: " + payload);
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("destination-topic", "key", "message"));
    }
}
