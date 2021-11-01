import kafka.gps.GpsService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.*;

public class GpsProducerAndConsumer_Test
{
    @Before
    public void run_prodAndStream()
    {
        GpsService gpsService = new GpsService();
        gpsService.run();
    }

    public Properties prop_settings()
    {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaProps.put("group.id", "gpsConsumer");
        kafkaProps.put("auto.offset.reset","earliest");
        kafkaProps.put("enable.auto.commit", "true");
        kafkaProps.put("auto.commit.interval.ms", "1000");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return kafkaProps;
    }

    @Test
    public void test_basicProducerAndConsumer()
    {
        ArrayList<String> tmp_list = new ArrayList<>();
        Properties properties = prop_settings();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        Set<String> topicNames = topics.keySet();
        for (String topic : topicNames)
        {
            String sub_str = topic.substring(0,7);
            System.out.println(sub_str);
            if (sub_str.equals("Tracker"))
            {
                tmp_list.add(topic);
            }
        }

        ArrayList<String> testList = new ArrayList<>(Arrays.asList("Tracker0","Tracker1", "Tracker2","Tracker3","Tracker4","Tracker5","Tracker6","Tracker7","Tracker8","Tracker9"));
        assertTrue(tmp_list.size() == testList.size() && tmp_list.containsAll(testList) && testList.containsAll(tmp_list));
    }

}
