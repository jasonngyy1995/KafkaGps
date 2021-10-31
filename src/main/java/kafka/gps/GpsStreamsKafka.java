package kafka.gps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

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


public class GpsStreamsKafka
{
    public String distanceCalculation(String oldLatitude, String oldLongitude, String newLatitude, String newLongitude)
    {
        double oldLatitudeDouble = Double.parseDouble(oldLatitude);
        double oldLongDouble = Double.parseDouble(oldLongitude);
        double newLatitudeDouble = Double.parseDouble(newLatitude);
        double newLongDouble = Double.parseDouble(newLongitude);

        if (oldLatitude.equals(newLatitude) && oldLongitude.equals(newLongitude))
        {
            return "0";
        } else {
            double theta = oldLongDouble - newLongDouble;
            double distance = Math.sin(Math.toRadians(oldLatitudeDouble)) * Math.sin(Math.toRadians(newLatitudeDouble)) +
                    Math.cos(Math.toRadians(oldLatitudeDouble)) * Math.cos(Math.toRadians(newLatitudeDouble)) * Math.cos(Math.toRadians(theta));
            // returns the arc cosine of an angle in between 0.0 and pi
            distance = Math.acos(distance);
            distance = Math.toDegrees(distance);
            distance = distance * 60 * 1.1515;
            distance = distance * 1.609344;

            String distance_inStr = Double.toString(distance);
            return distance_inStr;
        }

    }

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
    public static void firstSetOfStreams(StreamsBuilder gpsStreamer)
    {
        for (int i = 0; i < 10; i++)
        {
            KStream<String, String> gpsReading = gpsStreamer.stream("Tracker"+i);
            KStream<String, String> updatedValue = gpsReading.mapValues(value -> {
                List<String> tmp = Arrays.asList(value.split(","));
                List<String> tmp2 = new ArrayList<>(tmp);
                tmp2.remove(2);
                value = String.join(",", tmp2);
                return value;
            });

            updatedValue.to("SimpleTracker"+i);
        }
    }

    public static KStream<String, String> combine_stream(StreamsBuilder gpsStreamer)
    {
        KStream<String, String> combinedStream = null;
        for (int i = 0; i < 10; i++)
        {
            String topic_name = "Tracker"+i;
            KStream<String, String> newStream = gpsStreamer.stream(topic_name).map((key,value) ->  new KeyValue<>(topic_name+"_coordinates", value.toString()));
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
            String[] tmp = value.split(",");
            double event_lat = Double.parseDouble(tmp[0]);
            double event_long = Double.parseDouble(tmp[1]);

            if ((event_lat >= 39.5 && event_lat <= 40.5) && (event_long >= 115.5 && event_long <= 117.0))
            {
                return true;
            }
            return false;
        });

        GreaterBeijingAreaEvents.to("Beijing");
    }

    /* 1 set (10) of streams that take the Tracker0-9 topics and outputs the total distance travelled over the last 5 minutes for each topic respectively.
    The key for each new event should be "distance" from the start of the 5 minute period.
    The value should be a distance in meters
    Each new stream should output to a topic named "DistanceTrackerX", where X matches the input stream number. */
    public void thirdSetOfStream(StreamsBuilder gpsStreamer)
    {

    }

    public static void run()
    {
        Properties props = init_properties();
        StreamsBuilder gpsStreamer = new StreamsBuilder();
        firstSetOfStreams(gpsStreamer);
        secondSetOfStream(gpsStreamer);

        KafkaStreams streams = new KafkaStreams(gpsStreamer.build(), props);
        streams.cleanUp();
        streams.start();
    }

    public static void main(String[] args)
    {
        run();
    }

}
