package Bolt;

import Model.Publication;
import Model.Subscription;
import com.esotericsoftware.kryo.io.Output;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class RouteBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String type = tuple.getStringByField("type");
        String destinationTopic = tuple.getStringByField("destination-topic");
        String currentTopic = tuple.getStringByField("current-topic");
        if(type.startsWith("sub")){
            Subscription subscription = (Subscription) tuple.getValueByField("message");

        }else if(type.startsWith("pub")){
            Publication publication = (Publication) tuple.getValueByField("message");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("destination-topic", "key", "message"));
    }
}
