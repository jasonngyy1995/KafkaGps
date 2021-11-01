package kafka.gps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Scanner;

public class GpsViewer
{
    /* Prints each update from one of the input topics.
    Each update printed should be of the format TrackerX | Latitude: XX.XXX, Longitude: XXX.XXX, Altitude: XXXX.X
    When a user inputs a number (0-9), the client should change streams to the corresponding input.
    I.e if the user presses 1, the client should display Tracker 1's output */
    public static void run()
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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);

        System.out.println("Enter Tracker ID 0 - 9.");
        Scanner userInput = new Scanner(System.in);
        String trackerId = userInput.nextLine();
        String topic = "Tracker"+trackerId;

        if (Integer.parseInt(trackerId) < 0 || Integer.parseInt(trackerId) > 9)
        {
            System.out.println("Correct ID: 0 - 9");
            System.exit(0);
        }

        if (!trackerId.isEmpty())
        {
            consumer.subscribe(Arrays.asList(topic));
            try {
                while (true)
                {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords)
                    {
                        String[] value = record.value().split(",");
                        String lat = value[0];
                        String lon = value[1];
                        String alt = value[2];

                        System.out.printf("%s | Latitude: %s, Longitude: %s, Altitude: %s\n", record.topic(), lat, lon, alt);
                    }
                }
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args)
    {
        run();
    }
}

