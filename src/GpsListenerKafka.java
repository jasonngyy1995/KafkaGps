package kafka.gps;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.util.*;
import java.util.ArrayList;

/* There are 10 streams of GPS event, named "Tracker0"-"Tracker9"
Each stream should be sent to a Kafka topic with a corresponding name.
Each GPS event should be sent as a message to its corresponding steam
The key for each message should be "coordinates"
The value for each message should be a string containing the Latitude, Longitude & Altitude separated by commas
e.g. 40.0138816,116.3438099,154.2 */

class AsyncProducerCallback implements Callback
{
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e)
    {
        if (e != null)
        {
            System.out.println("Asyn producer succeed.");
        } else {
            System.out.println("Asyn producer failed.");
        }
    }
}

public class GpsListenerKafka implements GpsListener
{
    public ProducerRecord create_producerRecord(String name, String key, String value)
    {
        ProducerRecord<String, String> newProducerRecord = new ProducerRecord<>(name, key, value);
        return newProducerRecord;
    }


    @Override
    // name -> tracker id
    public void update(String name, double latitude, double longitude, double altitude) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("auto.create.topics.enable", true);

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        String long_str = Double.toString(longitude);
        String lat_str = Double.toString(latitude);
        String alt_str = Double.toString(altitude);

        String key = "coordinates";
        String value = lat_str + "," + long_str + "," + alt_str;
        ProducerRecord producerRecord = create_producerRecord(name, key, value);

        producer.send(producerRecord, new AsyncProducerCallback());
        System.out.println("Sent Completed.");
        System.out.println(name+": "+value);

        producer.flush();
        producer.close();
    }
}
