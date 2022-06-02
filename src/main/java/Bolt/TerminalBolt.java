package Bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TerminalBolt extends BaseRichBolt {

    private HashMap<String, Integer> count;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.count = new HashMap<>();
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Integer count = input.getIntegerByField("count");
        this.count.put(word, count);
        System.out.println("[TERMINAL_BOLT]RECEIVED WORD: " + word);
        this.collector.emit(new Values(word, count.toString()));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));
    }

    public void cleanup() {
        System.out.println("Topology Result:");
        for (Map.Entry<String, Integer> entry : this.count.entrySet()) {
            System.out.println(entry.getKey() + " - " + entry.getValue());
        }
    }

}
