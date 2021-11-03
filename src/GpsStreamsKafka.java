import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.nio.ByteBuffer;
import java.util.Map;
import java.time.Duration;
import java.util.*;

/* For the streams:

1 set (10) of streams that take the Tracker0-9 topics and remove the Altitude field.
e.g. 40.0138816,116.3438099,154.2 -> 40.0138816,116.3438099
Each new stream should output to a topic named "SimpleTrackerX", where X matches the input stream number.
The key for each event should remain the same.
1 stream that combines all input streams, and only outputs GPS events in the greater Beijing area (Latitude between 39.5-40.5,
Longitude between 115.5 -117.0)
The key for each event should be prepended with the name of the input stream.
1 set (10) of streams that take the Tracker0-9 topics and outputs the total distance travelled over the last 5 minutes for each topic respectively.
The key for each new event should be "distance" from the start of the 5 minute period.
The value should be a distance in meters
Each new stream should output to a topic named "DistanceTrackerX", where X matches the input stream number. */

class Distance
{
    String current_coord;
    double dist;

    Distance(String current_coord, double dist)
    {
        this.current_coord = current_coord;
        this.dist = dist;
    }

    String getCurrent_coord()
    {
        return this.current_coord;
    }

    double getDist()
    {
        return this.dist;
    }

    String getDistStr()
    {
        return String.valueOf(this.dist);
    }

    Double distanceCalculation(String newCoord)
    {
        String[] previous = this.current_coord.split(",");
        String[] current = newCoord.split(",");
        double oldLatitudeDouble = Double.parseDouble(previous[0]);
        double oldLongDouble = Double.parseDouble(previous[1]);
        double newLatitudeDouble = Double.parseDouble(current[0]);
        double newLongDouble = Double.parseDouble(current[1]);

        if (oldLatitudeDouble == newLatitudeDouble && oldLongDouble == newLongDouble)
        {
            return 0.0;
        } else {
            double theta = oldLongDouble - newLongDouble;
            double distance = Math.sin(Math.toRadians(oldLatitudeDouble)) * Math.sin(Math.toRadians(newLatitudeDouble)) +
                    Math.cos(Math.toRadians(oldLatitudeDouble)) * Math.cos(Math.toRadians(newLatitudeDouble)) * Math.cos(Math.toRadians(theta));
            // returns the arc cosine of an angle in between 0.0 and pi
            distance = Math.acos(distance);
            distance = Math.toDegrees(distance);
            distance = distance * 60 * 1.1515;
            distance = distance * 1.609344;

            return distance;
        }
    }

    void updateDistance(String newValue)
    {
        if (this.current_coord == null)
        {
            this.dist = 0.0;
        } else {
            this.dist = distanceCalculation(newValue);
        }
    }


}

class CustomSerializer implements Serializer<Distance>
{
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Distance distance) {

        int lengthOfCurrentCoord;
        byte[] serializedCurrentCoord;

        try {
            if (distance == null)
            {
                return null;
            }

            serializedCurrentCoord = distance.getCurrent_coord().getBytes(encoding);
            lengthOfCurrentCoord = serializedCurrentCoord.length;

            ByteBuffer buffer = ByteBuffer.allocate(8+4+lengthOfCurrentCoord);

            buffer.putDouble(distance.getDist());
            buffer.putInt(lengthOfCurrentCoord);
            buffer.put(serializedCurrentCoord);

            return buffer.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Distance to byte[]");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}

class CustomDeserializer implements Deserializer<Distance> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Nothing to configure
    }

    @Override
    public Distance deserialize(String topic, byte[] data) {

        try {
            if (data == null){
                System.out.println("Null received at deserialize");
                return null;
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            double dist = buffer.getDouble();

            int lengthOfCurrentCoord = buffer.getInt();
            byte[] currentCoordBytes = new byte[lengthOfCurrentCoord];
            buffer.get(currentCoordBytes);
            String deserializedCurrentCoord = new String(currentCoordBytes, encoding);

            return new Distance(deserializedCurrentCoord,dist);

        } catch (Exception e) {
            throw new SerializationException("Error when deserialize byte[] to Distance");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}

class DistanceSerde implements Serde<Distance> {

    private CustomSerializer customSerializer = new CustomSerializer();
    private CustomDeserializer customDeserializer = new CustomDeserializer();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        
    }

    @Override
    public void close() {
        customSerializer.close();
        customDeserializer.close();
    }

    @Override
    public Serializer<Distance> serializer() {
        return customSerializer;
    }

    @Override
    public Deserializer<Distance> deserializer() {
        return customDeserializer;
    }
}

public class GpsStreamsKafka
{
    // setting properties for StreamBuilder
    public static Properties init_properties()
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaGps");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return properties;
    }

    /* 1 set (10) of streams that take the Tracker0-9 topics and remove the Altitude field.
    e.g. 40.0138816,116.3438099,154.2 -> 40.0138816,116.3438099
    Each new stream should output to a topic named "SimpleTrackerX", where X matches the input stream number.
    The key for each event should remain the same. */
    public static void simplifyEachStream(StreamsBuilder streamer, int id)
    {
        KStream<String, String> gpsReading = streamer.stream("Tracker"+id);
        KStream<String, String> updatedValue = gpsReading.mapValues(value -> {
            List<String> tmp = Arrays.asList(value.split(","));
            List<String> tmp2 = new ArrayList<>(tmp);
            // remove altitude
            tmp2.remove(2);
            value = String.join(",", tmp2);
            return value;
        });

        // Materialize this stream to a new topic "SimpleTracker+id"
        updatedValue.to("SimpleTracker"+id);
    }

    public static void firstSetOfStreams(StreamsBuilder gpsStreamer)
    {
        // remove altitude for all trackers
        for (int i = 0; i < 10; i++)
        {
           simplifyEachStream(gpsStreamer,i);
        }
    }

    // function to merge record streams from all trackers
    public static KStream<String, String> combine_stream(StreamsBuilder gpsStreamer)
    {
        KStream<String, String> combinedStream = null;
        for (int i = 0; i < 10; i++)
        {
            String topic_name = "Tracker"+i;
            // prepended with the name of the input stream
            KStream<String, String> newStream = gpsStreamer.stream(topic_name).map((key,value) ->  new KeyValue<>(topic_name+"coordinates", value.toString()));
            if (i == 0)
            {
                combinedStream = newStream;
            } else {
                combinedStream = combinedStream.merge(newStream);
            }
        }
        return combinedStream;
    }

    /* 1 stream that combines all input streams, and only outputs GPS events in the greater Beijing area (Latitude between 39.5-40.5, Longitude between 115.5 -117.0)
    The key for each event should be prepended with the name of the input stream. */
    public static void secondSetOfStream(StreamsBuilder gpsStreamer)
    {
        KStream<String, String> combined_stream = combine_stream(gpsStreamer);
        KStream<String, String> GreaterBeijingAreaEvents = combined_stream.filter((key, value) -> {
            System.out.println(key);
            // process the value for comparison
            String[] tmp = value.split(",");
            double event_lat = Double.parseDouble(tmp[0]);
            double event_long = Double.parseDouble(tmp[1]);

            // if coordinates is within the given range
            if ((event_lat >= 39.5 && event_lat <= 40.5) && (event_long >= 115.5 && event_long <= 117.0))
            {
                return true;
            }
            return false;
        });

        // Materialize this stream to a new topic "Beijing"
        GreaterBeijingAreaEvents.to("Beijing");
    }

    /* 1 set (10) of streams that take the Tracker0-9 topics and outputs the total distance travelled over the last 5 minutes for each topic respectively.
    The key for each new event should be "distance" from the start of the 5 minute period.
    The value should be a distance in meters
    Each new stream should output to a topic named "DistanceTrackerX", where X matches the input stream number. */
    public static void calculateEachStream(StreamsBuilder streamsBuilder, int id)
    {
        DistanceSerde distanceSerde = new DistanceSerde();
        KStream<String, String> distanceReader = streamsBuilder.stream("Tracker"+id);
        distanceReader.groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5),Duration.ofSeconds(3)))
                .aggregate(() -> new Distance(null, 0.0), (key, value, aggregator) -> {
                            aggregator.updateDistance(value);
                            return aggregator;
                        }, Materialized.with(Serdes.String(),distanceSerde))
                .mapValues(Distance::getDistStr)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((key, value) -> KeyValue.pair("distance", value)).to("DistanceTracker"+id);
    }

    public static void thirdSetOfStream(StreamsBuilder gpsStreamer)
    {
        for (int i = 0; i < 10; i++)
        {
            calculateEachStream(gpsStreamer,i);
        }
    }

    public static void run()
    {
        Properties props = init_properties();
        StreamsBuilder gpsStreamer = new StreamsBuilder();
        firstSetOfStreams(gpsStreamer);
        secondSetOfStream(gpsStreamer);
//        thirdSetOfStream(gpsStreamer);

        // create Kafka client KafkaStreams to performing continuous computation on input
        KafkaStreams streams = new KafkaStreams(gpsStreamer.build(), props);
        // clean up the local StateStore directory of application id "KafkaGps"
        streams.cleanUp();
        // Start the KafkaStreams instance by starting all its thread
        streams.start();
    }

    public static void main(String[] args)
    {
        run();
    }

}