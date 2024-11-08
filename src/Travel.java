package sda.datastreaming;

import org.json.JSONObject;
import org.json.JSONArray;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class Travel {

    public static String processJson(String jsonString) {
        JSONObject jsonObject = new JSONObject(jsonString);
        JSONArray dataArray = jsonObject.getJSONArray("data");
        JSONObject firstEntry = dataArray.getJSONObject(0);
        JSONObject clientProperties = firstEntry.getJSONObject("properties-client");
        JSONObject driverProperties = firstEntry.getJSONObject("properties-driver");

        // Extract coordinates
        double clientLongitude = clientProperties.getDouble("longitude");
        double clientLatitude = clientProperties.getDouble("latitude");
        double driverLongitude = driverProperties.getDouble("longitude");
        double driverLatitude = driverProperties.getDouble("latitude");

        // Calculate distance
        double distance = calculateDistance(clientLatitude, clientLongitude, driverLatitude, driverLongitude);

        // Round distance to 3 decimal places
        BigDecimal roundedDistance = new BigDecimal(distance).setScale(3, RoundingMode.HALF_UP);

        // Extract price per km
        double prixBasePerKm = firstEntry.getDouble("prix_base_per_km");

        // Calculate the total price of the trip
        BigDecimal prixTravel = new BigDecimal(prixBasePerKm).multiply(new BigDecimal(roundedDistance.toString()));

        // Modify JSON to include "location", "distance", and "prix_travel"
        clientProperties.remove("longitude");
        clientProperties.remove("latitude");
        driverProperties.remove("longitude");
        driverProperties.remove("latitude");

        String clientLocation = clientLongitude + ", " + clientLatitude;
        String driverLocation = driverLongitude + ", " + driverLatitude;

        clientProperties.put("location", clientLocation);
        driverProperties.put("location", driverLocation);

        firstEntry.put("distance", roundedDistance);
        firstEntry.put("prix_travel", prixTravel.setScale(2, RoundingMode.HALF_UP));

        return jsonObject.toString(4); // Returns the modified JSON in a readable format
    }

    // Method to calculate distance using the Haversine formula
    public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        double EARTH_RADIUS = 6371; // Earth radius in km

        return EARTH_RADIUS * c;
    }
}
