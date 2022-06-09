package Bolt;

import Database.DatabaseService;
import Database.MyMongoClient;
import Model.Subscription;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class PersistentBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private DatabaseService databaseService;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        databaseService = DatabaseService.getInstance();
    }

    @Override
    public void execute(Tuple tuple) {
        String responseTopic = tuple.getStringByField("source");
        String subscriptionJson = tuple.getStringByField("subscription");
        System.out.println("Source: " + responseTopic + " sub: " + subscriptionJson);
        databaseService.addSub(responseTopic, subscriptionJson);
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
