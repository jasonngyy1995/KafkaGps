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
    public void testOriginalTopic()
    {
        Properties props = prop_settings();
        String topic_name = "Tracker1";
        KafkaConsumer<String, String> StreamTester = new KafkaConsumer<>(props);
        StreamTester.subscribe(Arrays.asList(topic_name));
        int i = 0;
        while (i < 20)
        {
            ConsumerRecords<String, String> consumerRecords = StreamTester.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords)
            {
                String[] value_str = record.value().split(",");
                assertEquals(value_str.length, 3);
                assertEquals(record.topic(), "Tracker1");
                System.out.printf("key = %s, value = %s\n", record.key(), record.value());

                i++;
            }
        }
    }

    @Test
    public void seeInputPrintResult()
    {
        final Runnable printGpsViewerResult = new Thread(() -> {
            ByteArrayInputStream in = new ByteArrayInputStream("5".getBytes());
            System.setIn(in);

            GpsViewerKafka gpsViewerKafka = new GpsViewerKafka();
            gpsViewerKafka.run();
        });

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future future = executor.submit(printGpsViewerResult);
        executor.shutdown(); // This does not cancel the already-scheduled task.

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
            if (sub_str.equals("Tracker"))
            {
                tmp_list.add(topic);
            }
        }

        ArrayList<String> testList = new ArrayList<>(Arrays.asList("Tracker0","Tracker1", "Tracker2","Tracker3","Tracker4","Tracker5","Tracker6","Tracker7","Tracker8","Tracker9"));
        assertTrue(tmp_list.size() == testList.size() && tmp_list.containsAll(testList) && testList.containsAll(tmp_list));
    }
}
