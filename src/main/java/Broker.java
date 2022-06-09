import Bolt.*;
import Utils.BrokerInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;

public class Broker {
    private static final String KAFKA_SPOUT_ID = "kafka_spout";
    private static final String KAFKA_BOLT_ID = "kafka_bolt";
    private static final String PREPARE_BOLT_ID = "prepare_bolt";
    private static final String PERSISTENT_BOLT_ID = "persistent_bolt";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        MatchBolt matchBolt = new MatchBolt();
        PersistentBolt persistentBolt = new PersistentBolt();

        String topic = args[0];
        BrokerInfo.BROKER_TOPIC_ID = topic;
        System.out.println("INITIALIZED LISTENING TOPIC FOR BROKER: " + topic);
        String port = "9092";

        //KAFKA SPOUT
        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("localhost:" + port, topic);
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "topic-brokers");// + "-" + UUID.randomUUID());
        spoutConfigBuilder.setProp(prop);
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();

        //KAFKA BOLT
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt kafkaBolt = new KafkaBolt()
                .withProducerProperties(props)
                .withTopicSelector(new MyKafkaTopicSelector())
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout<>(spoutConfig), 4);
        builder.setBolt(PREPARE_BOLT_ID, matchBolt, 4).setNumTasks(8).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(PERSISTENT_BOLT_ID, persistentBolt).directGrouping(PREPARE_BOLT_ID, "persist");
        builder.setBolt(KAFKA_BOLT_ID, kafkaBolt, 4).setNumTasks(8).shuffleGrouping(PREPARE_BOLT_ID);

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();

        // fine tuning
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        config.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE, 1);

        cluster.submitTopology("count_topology", config, topology);

//        try {
//            Thread.sleep(20000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        cluster.killTopology("count_topology");
//        cluster.shutdown();

    }
}