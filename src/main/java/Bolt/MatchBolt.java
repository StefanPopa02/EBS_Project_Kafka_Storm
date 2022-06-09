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
import java.util.concurrent.ThreadLocalRandom;

public class MatchBolt extends BaseRichBolt {

    private int matchCount;
    private OutputCollector outputCollector;
    private Gson gson;

    private Map<String, List<Subscription>> routingTable;
    private Map<String, List<String>> neighborsTopicList;
    private List<Integer> directTasks;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        directTasks = topologyContext.getComponentTasks("persistent_bolt");
        matchCount = 0;
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
            String[] keyComponents = responseTopic.split("-", 3);
            String pubId = keyComponents[1];
            String fromBrokerTopic = keyComponents[2];
            Publication publication = gson.fromJson(payload, Publication.class);
//            System.out.println("[BROKER]PUBLICATION RECEIVED: " + responseTopic + " value: " + publication);
            for (Map.Entry<String, List<Subscription>> entry : routingTable.entrySet()) {
                if (entry.getKey().equals(fromBrokerTopic)) {
                    continue;
                }
                for (Subscription subscription : entry.getValue()) {
                    //Not the best algo => if we have 2 equal subscriptions it will send the publication twice
                    //But in our case we don't have 2 equal subscriptions :)
                    if (isMatching(subscription, publication)) {
                        matchCount++;
                        this.outputCollector.emit(new Values(entry.getKey(), "pub-" + pubId + "-" + currentTopic, payload));
//                        System.out.println("[BROKER]PUBLICATION SENT: " + currentTopic + " -> " + entry.getKey());
//                        System.out.println("Matched: " + matchCount);
                        break;
                    }
                }
            }
        } else {
            //SUBSCRIPTION RECEIVED FROM BROKER/SUB (WE TREAT THEM THE SAME)
            Subscription subscription = gson.fromJson(payload, Subscription.class);
//            System.out.println("[BROKER]SUBSCRIPTION RECEIVED listening response on topic: " + responseTopic + " value: " + subscription);
            //Add (subscriber/broker topic, message) to routing table
            List<Subscription> existingSubs = routingTable.computeIfAbsent(responseTopic, k -> new ArrayList<>());
            existingSubs.add(subscription);
//            int randomTaskNum = ThreadLocalRandom.current().nextInt(0, directTasks.size());
//            this.outputCollector.emitDirect(directTasks.get(randomTaskNum), "persist", new Values(responseTopic, payload));
            // Foreach neighbor broker emit tuple with the (key, message)
            // key = source = current broker
            List<String> neighborsBrokers = neighborsTopicList.get(currentTopic);
            for (String neighborBrokerTopic : neighborsBrokers) {
                if (!responseTopic.equals(neighborBrokerTopic)) {
                    this.outputCollector.emit(new Values(neighborBrokerTopic, currentTopic, payload));
//                    System.out.println("[BROKER]SUBSCRIPTION SENT: " + currentTopic + " -> " + neighborBrokerTopic + " value: " + payload);
                }
            }
        }

        this.outputCollector.ack(tuple);
    }

    private boolean isMatching(Subscription subscription, Publication publication) {
        Map<String, String> fieldOp = subscription.getFieldOperator();
        return checkMultipleTypesMatch(subscription.getCompany(), publication.getCompany(), fieldOp.get("Company"))
                && checkMultipleTypesMatch(subscription.getDate(), publication.getDate(), fieldOp.get("Date"))
                && checkMultipleTypesMatch(subscription.getDrop(), publication.getDrop(), fieldOp.get("Drop"))
                && checkMultipleTypesMatch(subscription.getValue(), publication.getValue(), fieldOp.get("Value"))
                && checkMultipleTypesMatch(subscription.getVariation(), publication.getVariation(), fieldOp.get("Variation"));
    }

    private boolean checkMultipleTypesMatch(Double subDouble, Double pubDouble, String fieldOp) {
        if (fieldOp == null) {
            return true;
        }

        switch (fieldOp) {
            case ">":
                return pubDouble.compareTo(subDouble) > 0;
            case "<":
                return pubDouble.compareTo(subDouble) < 0;
            case "=":
                return pubDouble.compareTo(subDouble) == 0;
            case ">=":
                return pubDouble.compareTo(subDouble) > 0 || pubDouble.compareTo(subDouble) == 0;
            case "<=":
                return pubDouble.compareTo(subDouble) < 0 || pubDouble.compareTo(subDouble) == 0;
            case "!=":
                return pubDouble.compareTo(subDouble) != 0;
        }

        return false;
    }

    private boolean checkMultipleTypesMatch(Date subDate, Date pubDate, String fieldOp) {
        if (fieldOp == null) {
            return true;
        }

        switch (fieldOp) {
            case ">":
                return pubDate.compareTo(subDate) > 0;
            case "<":
                return pubDate.compareTo(subDate) < 0;
            case "=":
                return pubDate.compareTo(subDate) == 0;
            case ">=":
                return pubDate.compareTo(subDate) > 0 || pubDate.compareTo(subDate) == 0;
            case "<=":
                return pubDate.compareTo(subDate) < 0 || pubDate.compareTo(subDate) == 0;
            case "!=":
                return pubDate.compareTo(subDate) != 0;
        }

        return false;
    }

    private boolean checkMultipleTypesMatch(String subString, String pubString, String fieldOp) {
        if (fieldOp == null) {
            return true;
        }

        switch (fieldOp) {
            case ">":
                return subString.compareTo(pubString) > 0;
            case "<":
                return subString.compareTo(pubString) < 0;
            case "=":
                return subString.compareTo(pubString) == 0;
            case ">=":
                return subString.compareTo(pubString) > 0 || subString.compareTo(pubString) == 0;
            case "<=":
                return subString.compareTo(pubString) < 0 || subString.compareTo(pubString) == 0;
            case "!=":
                return subString.compareTo(pubString) != 0;
        }

        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("destination-topic", "key", "message"));

//        outputFieldsDeclarer.declareStream("persist", true, new Fields("source", "subscription"));
    }
}
