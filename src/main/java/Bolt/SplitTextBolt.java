package Bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitTextBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;

    }

    public void execute(Tuple input) {
        // TODO Auto-generated method stub
        System.out.println("[SPLIT_BOLT]GOT TUPLE " + input);
        System.out.println("FIELDS: " + input.getFields().toString());
        System.out.println("VALUES: " + input.getValues().toString());
        String key = input.getStringByField("key");
        if (key != null && key.equals("words")) {
            String value = input.getStringByField("value");
            String[] words = value.split(" ");
            for (String word : words) {
                this.collector.emit(new Values(word));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
