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

public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Integer> count;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.count = new HashMap<>();

    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Integer wordcount = this.count.get(word);

        if (wordcount == null) {
            wordcount = 0;
        }
        wordcount++;
        this.count.put(word, wordcount);
        this.collector.emit(new Values(word, wordcount));
        System.out.println("[WORD_BOLT]Word: " + word + " count: " + wordcount);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}
