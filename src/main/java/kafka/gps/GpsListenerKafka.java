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
            System.out.println("Asyn producer succeed1.");
        } else {
            System.out.println("Asyn producer failed.");
        }
    }
}

public class GpsListenerKafka implements GpsListener
{
    KafkaProducer<String, String> producer;
    AdminClient adminClient;
    ArrayList<String> createdTopic;

    public void init_producerProperties()
    {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(kafkaProps);
    }

    public void init_adminClient()
    {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        adminClient.create(properties);
        adminClient.close(Duration.ofSeconds(30));
    }

    public ProducerRecord create_producerRecord(String name, String key, String value)
    {
        ProducerRecord<String, String> newProducerRecord = new ProducerRecord<>(name, key, value);
        return newProducerRecord;
    }

    public CreateTopicsResult createTopic(String trackerID)
    {
        int partition = 1;
        short replica = 1;
        CreateTopicsResult newTopic = adminClient.createTopics(Collections.singletonList(

                new NewTopic(trackerID, partition, replica))
        );

        return newTopic;
    }

    public boolean checkIfTopicExists(String topicToCheck)
    {
        for (String topic: createdTopic)
        {
            if (topicToCheck.equals(topic))
            {
                return true;
            }
        }
        return false;
    }


    @Override
    // name -> tracker id
    public void update(String name, double latitude, double longitude, double altitude) {
        boolean existingTopic = checkIfTopicExists(name);
        if (existingTopic == false)
        {
            createdTopic.add(name);
            createTopic(name);
        }

        System.out.println("Testing: print all existing topics");
        createdTopic.forEach(System.out::println);


        init_producerProperties();

        String long_str = Double.toString(longitude);
        String lat_str = Double.toString(latitude);
        String alt_str = Double.toString(altitude);

        String key = "coordinates";
        String value = lat_str + "," + long_str + "," + alt_str;
        ProducerRecord producerRecord = create_producerRecord(name, key, value);

        producer.send(producerRecord, new AsyncProducerCallback());
        System.out.println("Sent Completed.");

        producer.flush();
        producer.close();
    }
}
