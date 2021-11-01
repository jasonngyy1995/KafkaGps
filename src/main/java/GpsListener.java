public interface GpsListener {

    /**
     * Called whenever a GPS sends an update.
     *
     * @param   name A String that identifies the GPS device sending the update.
     * @param   latitude The Latitude of the GPS device at the time of the update.
     * @param   longitude The Longitude of the GPS device at the time of the update.
     * @param   altitude The Altitude (in feet) of the GPS device at the time of the update.
     */
    public void update(String name, double latitude, double longitude, double altitude);

} 
