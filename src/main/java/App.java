import Bolt.SplitTextBolt;
import Bolt.TerminalBolt;
import Bolt.WordCountBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Properties;

public class App
{
    private static final String KAFKA_SPOUT_ID = "kafka_spout";
    private static final String SPLIT_BOLT_ID = "split_bolt";
    private static final String COUNT_BOLT_ID = "count_bolt";
    private static final String TERMINAL_BOLT_ID = "terminal_bolt";

    public static void main( String[] args ) throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        SplitTextBolt splitbolt = new SplitTextBolt();
        WordCountBolt countbolt = new WordCountBolt();
        TerminalBolt terminalbolt = new TerminalBolt();

        String port = "9092";
        String topic = "Hello-Kafka";

        KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("localhost:" + port, topic);
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "words");
        spoutConfigBuilder.setProp(prop);
        KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();

        builder.setSpout(KAFKA_SPOUT_ID, new KafkaSpout<>(spoutConfig));
        builder.setBolt(SPLIT_BOLT_ID, splitbolt).shuffleGrouping(KAFKA_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countbolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(TERMINAL_BOLT_ID, terminalbolt).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();

        // fine tuning
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,1024);
        config.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE,1);

        cluster.submitTopology("count_topology", config, topology);

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology("count_topology");
        cluster.shutdown();

    }
}