package Bolt;

import Database.DatabaseService;
import Util.BrokerInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class PersistentBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private DatabaseService databaseService;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
//        databaseService = DatabaseService.getInstance();
    }

    @Override
    public void execute(Tuple tuple) {
        String responseTopic = tuple.getStringByField("source");
        String subscriptionJson = tuple.getStringByField("subscription");
//        databaseService.addSub(responseTopic, subscriptionJson);
        try {
            persistSubToFile(responseTopic, subscriptionJson);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void persistSubToFile(String source, String subscriptionJson) throws IOException {
        FileWriter fileWriter = new FileWriter("persistData/" + BrokerInfo.BROKER_TOPIC_ID + "/" + source, true);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(subscriptionJson + "\n");
        printWriter.close();
    }
}
