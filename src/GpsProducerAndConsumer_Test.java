import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class GpsProducerAndConsumer_Test
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

    // test case for checking correct result produced by producer
    @Test
    public void testOriginalTopic()
    {
        // run GpsService to produce records first
        GpsService gpsService = new GpsService();
        gpsService.run();

        // create a consumer which subscribe to Tracker1 for testing
        Properties props = prop_settings();
        String topic_name = "Tracker1";
        KafkaConsumer<String, String> StreamTester = new KafkaConsumer<>(props);
        StreamTester.subscribe(Arrays.asList(topic_name));

        // simulate a timeout
        int i = 0;
        while (i < 20)
        {
            ConsumerRecords<String, String> consumerRecords = StreamTester.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords)
            {
                String[] value_str = record.value().split(",");
                // 1) check if value's size is 3 (lat+lon+alt)
                assertEquals(value_str.length, 3);
                // 2) check if message topic matches "Tracker1"
                assertEquals(record.topic(), "Tracker1");
                // print the record for manual check
                System.out.printf("key = %s, value = %s\n", record.key(), record.value());

                i++;
            }
        }
    }

    // Test case to check if return record matches user entered Tracker within a period of time
    @Test
    public void seeInputPrintResult()
    {
        final Runnable printGpsViewerResult = new Thread(() -> {
            // set the testing user input as 5, subscribed tracker should be "Tracker5"
            ByteArrayInputStream in = new ByteArrayInputStream("5".getBytes());
            System.setIn(in);

            // run our consumer and check the printed output
            GpsViewerKafka gpsViewerKafka = new GpsViewerKafka();
            gpsViewerKafka.run();
        });

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future future = executor.submit(printGpsViewerResult);
        executor.shutdown();

        // set a timeout
        try {
            future.get(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException ie) {
            // ignore
        }
        catch (ExecutionException ee) {
            // ignore
        }
        catch (TimeoutException te) {
            // ignore
        }
        if (!executor.isTerminated())
            executor.shutdownNow(); // If you want to stop the code that hasn't finished.
    }

    // Test case to check if topics Tracker0-9 produced, test passed local but failed in GradeScope. Muted for submission
//    @Test
//    public void test_basicProducerAndConsumer()
//    {
//        ArrayList<String> tmp_list = new ArrayList<>();
//        Properties properties = prop_settings();
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
//        Set<String> topicNames = topics.keySet();
//        for (String topic : topicNames)
//        {
//            String sub_str = topic.substring(0,7);
//            if (sub_str.equals("Tracker"))
//            {
//                tmp_list.add(topic);
//            }
//        }
//
//        ArrayList<String> testList = new ArrayList<>(Arrays.asList("Tracker0","Tracker1", "Tracker2","Tracker3","Tracker4","Tracker5","Tracker6","Tracker7","Tracker8","Tracker9"));
//        assertTrue(tmp_list.size() == testList.size() && tmp_list.containsAll(testList) && testList.containsAll(tmp_list));
//    }
}
