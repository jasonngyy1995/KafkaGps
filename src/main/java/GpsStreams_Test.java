import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.*;
import static org.junit.Assert.*;
import java.util.*;
import java.time.Duration;

public class GpsStreams_Test
{
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

    @Before
    public void run_prodAndStream()
    {
        GpsService gpsService = new GpsService();
        gpsService.run();

        GpsStreamsKafka gpsStreamsKafka = new GpsStreamsKafka();
        gpsStreamsKafka.run();
    }

    @Test
    public void test_RemovingAltitude()
    {
        // SimpleTracker9 as an example
        Properties props = prop_settings();
        String topic_name = "SimpleTracker9";
        KafkaConsumer<String, String> firstStreamTester = new KafkaConsumer<>(props);
        firstStreamTester.subscribe(Arrays.asList(topic_name));
        int i = 0;
        while (i < 20)
        {
            ConsumerRecords<String, String> consumerRecords = firstStreamTester.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords)
            {
                String[] value_str = record.value().split(",");
                assertEquals(value_str.length, 2);
                assertEquals(record.topic(), "SimpleTracker9");
                System.out.printf("key = %s, value = %s\n", record.key(), record.value());

                i++;
            }
        }
    }

    @Test
    public void test_BeijingOnlyStreams()
    {
        ArrayList<Double> beijingLat = new ArrayList<>();
        ArrayList<Double> beijingLong = new ArrayList<>();

        Properties props = prop_settings();
        String topic_name = "Beijing";
        KafkaConsumer<String, String> secondStreamTester = new KafkaConsumer<>(props);
        secondStreamTester.subscribe(Arrays.asList(topic_name));
        int i = 0;
        while (i < 20)
        {
            ConsumerRecords<String, String> consumerRecords = secondStreamTester.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords)
            {
                assertEquals(record.topic(), "Beijing");
                String[] value_str = record.value().split(",");
                double lat_d = Double.parseDouble(value_str[0]);
                double long_d = Double.parseDouble(value_str[1]);
                beijingLat.add(lat_d);
                beijingLong.add(long_d);
                System.out.printf("key = %s, value = %s\n", record.key(), record.value());

                i++;
            }
        }

        for (Double lat_record : beijingLat)
        {
            assertTrue(String.valueOf(lat_record), (lat_record >= 39.5 && lat_record <= 40.5));
        }

        for (Double long_record : beijingLong)
        {
            assertTrue(String.valueOf(long_record), (long_record >= 115.5 && long_record <= 117.0));
        }
    }
}
