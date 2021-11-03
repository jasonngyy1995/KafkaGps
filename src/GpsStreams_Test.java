import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.*;
import static org.junit.Assert.*;
import java.util.*;
import java.time.Duration;

public class GpsStreams_Test
{
    // settings for consumer properties
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

    // run GpsService and GpsStreamsKafka to produce records first
    @Before
    public void run_prodAndStream()
    {
        GpsService gpsService = new GpsService();
        gpsService.run();

        GpsStreamsKafka gpsStreamsKafka = new GpsStreamsKafka();
        gpsStreamsKafka.run();
    }

    // test case to check if altitude is removed
    @Test
    public void test_RemovingAltitude()
    {
        // SimpleTracker9 as an example
        Properties props = prop_settings();
        String topic_name = "SimpleTracker9";
        KafkaConsumer<String, String> firstStreamTester = new KafkaConsumer<>(props);
        firstStreamTester.subscribe(Arrays.asList(topic_name));

        // simulate a timeout
        int i = 0;
        while (i < 20)
        {
            ConsumerRecords<String, String> consumerRecords = firstStreamTester.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords)
            {
                String[] value_str = record.value().split(",");
                // 1) check if value's size is 2 (lat+lon)
                assertEquals(value_str.length, 2);
                // 2) check if message topic matches "SimpleTracker9"
                assertEquals(record.topic(), "SimpleTracker9");
                // print the record for manual check
                System.out.printf("key = %s, value = %s\n", record.key(), record.value());

                i++;
            }
        }
    }

    // Test case to check value of record from topic "Beijing" is filtered correctly
    @Test
    public void test_BeijingOnlyStreams()
    {
        Properties props = prop_settings();
        String topic_name = "Beijing";
        KafkaConsumer<String, String> secondStreamTester = new KafkaConsumer<>(props);
        secondStreamTester.subscribe(Arrays.asList(topic_name));

        // simulate a timeout
        int i = 0;
        while (i < 20)
        {
            ConsumerRecords<String, String> consumerRecords = secondStreamTester.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords)
            {
                // 1) check if message topic matches "Beijing"
                assertEquals(record.topic(), "Beijing");
                String[] value_str = record.value().split(",");
                // 2) check if value within the range of Greater Beijing Area
                double lat_d = Double.parseDouble(value_str[0]);
                System.out.println("lat: "+lat_d);
                assertTrue(lat_d >= 39.5 && lat_d <= 40.5);
                double long_d = Double.parseDouble(value_str[1]);
                System.out.println("lon: "+long_d);
                assertTrue(long_d >= 115.5 && long_d <= 117.0);
                // print the record for manual check
                System.out.printf("topic = %s, key = %s, value = %s\n", record.topic(), record.key(), record.value());
                i++;
            }
        }
    }
}

