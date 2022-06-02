package Bolt;

import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.apache.storm.tuple.Tuple;

public class MyKafkaTopicSelector implements KafkaTopicSelector {

    @Override
    public String getTopic(Tuple tuple) {
        String topicToSend = tuple.getStringByField("destination-topic");
        return topicToSend;
    }
}
