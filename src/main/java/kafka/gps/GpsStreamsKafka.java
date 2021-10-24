package kafka.gps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Arrays;
import java.util.Properties;

/* For the streams:

1 set (10) of streams that take the Tracker0-9 topics and remove the Altitude field.
e.g. 40.0138816,116.3438099,154.2 -> 40.0138816,116.3438099
Each new stream should output to a topic named "SimpleTrackerX", where X matches the input stream number.
The key for each event should remain the same.
1 stream that combines all input streams, and only outputs GPS events in the greater Beijing area (Latitude between 39.5-40.5, Longitude between 115.5 -117.0)
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

    public static void firstSetOfStreams()
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaGps");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        for (int i = 0; i < 10; i++)
        {
            StreamsBuilder gpsStreamer = new StreamsBuilder();
            KStream<String, String> gpsReading = gpsStreamer.stream("Tracker"+1);

            KStream updatedValue = gpsReading.mapValues(value -> String.join(",",Arrays.asList(value.split(","))
                    .remove(2)));

            updatedValue.to("SimpleTracker"+i);
        }
    }

    public static void thirdSetOfStreams()
    {

    }

    public static void main(String[] args) throws Exception
    {
        firstSetOfStreams();
    }
}
