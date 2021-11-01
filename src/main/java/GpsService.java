import java.io.*;
import java.util.*;

/**
 * Simulates live GPS data by playing back records from the Geolife data set.
 * @author Ian Knight
 * @version 1.0
 */
public class GpsService {

    public static void main(String[] args) {
        GpsService service = new GpsService();
        service.run();
    }

    /**
     * Reads binary data from the gps.dat file and plays that data back at the intervals given in the data set.
     */
    public void run(){

        LinkedList<Double[]>[] data;
        LinkedList<Timer> timers = new LinkedList<Timer>();
        Timer t;
        GpsEvent ev;

        // Read the data file
        try {   
            FileInputStream fileIn = new FileInputStream("src/main/java/gps.dat");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            data = (LinkedList<Double[]>[]) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            return;
        } catch (ClassNotFoundException c) {
            System.out.println("Data not found");
            c.printStackTrace();
            return;
        }

        // Setup and begin playback of records for each GPS Tracker
        for(int i=0; i<data.length; i++){    
            t = new Timer();
            timers.add(t);
            ev = new GpsEvent(i,data[i],t,new GpsListenerKafka());
            t.schedule(ev,1000);
        }
    }

    /**
     * Asynchronous runnable class for playing back events at the corresponding time.
     */
    private class GpsEvent extends TimerTask {

        public int id = 0;
        public LinkedList<Double[]> data;
        public Timer timer;
        public GpsListener listener;

        public GpsEvent(int id, LinkedList<Double[]> data, Timer timer, GpsListener listener){
            this.id = id;
            this.data = data;
            this.timer = timer;
            this.listener = listener;
        }

        public void run() {
            Double[] event = data.poll();
            data.add(event);
            Double[] next = data.peek();

            listener.update("Tracker"+id,event[0].doubleValue(),event[1].doubleValue(),event[2].doubleValue());
            timer.schedule(new GpsEvent(id,data,timer,listener),next[3].longValue()*1000);
        }
    }

}
