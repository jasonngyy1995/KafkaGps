package kafka.gps;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Scanner;

public class GpsConsumer
{
    /* Prints each update from one of the input topics.
    Each update printed should be of the format TrackerX | Latitude: XX.XXX, Longitude: XXX.XXX, Altitude: XXXX.X
    When a user inputs a number (0-9), the client should change streams to the corresponding input.
    I.e if the user presses 1, the client should display Tracker 1's output */
    public static void main(String[] args)
    {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "broker1:9092,broker2:9092");
        kafkaProps.setProperty("group.id", "CountryCounter");
        kafkaProps.setProperty("enable.auto.commit", "true");
        kafkaProps.setProperty("auto.commit.interval.ms", "1000");
        kafkaProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);

        Scanner userInput = new Scanner(System.in);
        String trackerId = userInput.nextLine();

        while (!trackerId.isEmpty())
        {
            consumer.subscribe(Arrays.asList("Tracker"+trackerId));
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

                        System.out.printf("Tracker%s | Latitude: %s, Longitude: %s, Altitude: %s\n", trackerId, lat, lon, alt);
//                        System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    }
                }
            } finally {
                consumer.close();
            }
        }
    }
}
