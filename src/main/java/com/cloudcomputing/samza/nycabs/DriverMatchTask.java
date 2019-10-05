package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.WindowableTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.HashMap;
import java.util.Map;


/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */
    private double MAX_MONEY = 100.0;

    // key is blockId, map key is driver really id
    private KeyValueStore<String, Map<String, Map<String, Object>>> driver_loc;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe the kv stores?)
        driver_loc = (KeyValueStore<String, Map<String, Map<String, Object>>>) context.getTaskContext().getStore("driver-loc");
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        HashMap<String, Object> message = new HashMap<String, Object>();
        message.put("event", "postMessage");
        message.put("sender", "planck");
        message.put("text", "Hello world");
        collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, message));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
//        String incomingStream = envelope.getSystemStreamPartition().getStream();
//        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
//            processDriverLocEvent((Map<String, Object>) envelope.getMessage());
//        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
//            // Handle Event messages
//            processNormalEvent((Map<String, Object>) envelope.getMessage(), collector);
//        } else {
//            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
//        }
    }

    public void processDriverLocEvent(Map<String, Object> message) {
        if (!message.get("type").equals("DRIVER_LOCATION")) {
            throw new IllegalStateException("Unexpected event type on messages stream: " + message.get("event"));
        }
        String blockId = String.valueOf(message.get("blockId"));
        String driveId = String.valueOf(message.get("driverId"));
        Map<String, Map<String, Object>> driveMapInOneBlock = driver_loc.get(blockId);
        if (driveMapInOneBlock == null) {
            driveMapInOneBlock = new HashMap<String, Map<String, Object>>();
        }
        Map<String, Object> driveInfo;
        if (driveMapInOneBlock.containsKey(driveId)) {
            driveInfo = driveMapInOneBlock.get(driveId);
        } else {
            driveInfo = new HashMap<String, Object>();
        }
        driveInfo.put("latitude", String.valueOf(message.get("latitude")));
        driveInfo.put("longitude", String.valueOf(message.get("longitude")));
        driveMapInOneBlock.put(driveId, driveInfo);
        driver_loc.put(blockId, driveMapInOneBlock);
    }

    public void processNormalEvent(Map<String, Object> message, MessageCollector collector) {
        String eventType = (String) message.get("type");
        if (eventType.equals("DRIVER_LOCATION")) {
            throw new IllegalStateException("Unexpected event type on messages stream: " + eventType);
        }
        String blockId = String.valueOf(message.get("blockId"));
        if (eventType.equals("LEAVING_BLOCK")) {
            String driveId = String.valueOf(message.get("driverId"));
            System.out.println(driveId + " is leaving " + blockId);
            Map<String, Map<String, Object>> driveMapInOneBlock = driver_loc.get(blockId);
            if (driveMapInOneBlock != null && driveMapInOneBlock.containsKey(driveId)) {
                driveMapInOneBlock.remove(driveId);
            }
            driver_loc.put(blockId, driveMapInOneBlock);
        } else if (eventType.equals("ENTERING_BLOCK")) {
            String driveId = String.valueOf(message.get("driverId"));
            System.out.println(driveId + " is entering " + blockId);
            Map<String, Map<String, Object>> driveMapInOneBlock = driver_loc.get(blockId);
            if (driveMapInOneBlock == null) {
                driveMapInOneBlock = new HashMap<String, Map<String, Object>>();
            }
            Map<String, Object> driverInfo = new HashMap<>();
            driverInfo.put("latitude", String.valueOf(message.get("latitude")));
            driverInfo.put("longitude", String.valueOf(message.get("longitude")));
            driverInfo.put("gender", (String) message.get("gender"));
            driverInfo.put("rating", String.valueOf(message.get("rating")));
            driverInfo.put("salary", String.valueOf(message.get("salary")));
            driverInfo.put("status", (String) message.get("status"));
            driveMapInOneBlock.put(driveId, driverInfo);
            driver_loc.put(blockId, driveMapInOneBlock);
        } else if (eventType.equals("RIDE_COMPLETE")) {
            String driveId = String.valueOf(message.get("driverId"));
            Map<String, Map<String, Object>> driveMapInOneBlock = driver_loc.get(blockId);
            if (driveMapInOneBlock == null) {
                driveMapInOneBlock = new HashMap<String, Map<String, Object>>();
            }
            System.out.println(driveId + " complete order become available");
            Map<String, Object> driverInfo;
            if (driveMapInOneBlock.containsKey(driveId)) {
                driverInfo = driveMapInOneBlock.get(driveId);
            } else {
                driverInfo = new HashMap<String, Object>();
            }
            driverInfo.put("latitude", String.valueOf(message.get("latitude")));
            driverInfo.put("longitude", String.valueOf(message.get("longitude")));
            driverInfo.put("gender", (String) message.get("gender"));
            double oldRating = (Double) message.get("rating");
            double userRating = (Double) message.get("user_rating");
            driverInfo.put("rating", String.valueOf((oldRating + userRating) / 2));
            driverInfo.put("salary", String.valueOf(message.get("salary")));
            driverInfo.put("status", "AVAILABLE");
            driveMapInOneBlock.put(driveId, driverInfo);
            driver_loc.put(blockId, driveMapInOneBlock);
        } else if (eventType.equals("RIDE_REQUEST")) {
            String clientId = String.valueOf(message.get("clientId"));
            String genderPreferennce = (String) message.get("gender_preference");
            Double clientLatitude = (Double) message.get("latitude");
            Double clientLongitude = (Double) message.get("longitude");
            String highestScoreDriverId = null;
            Double highestScore = Double.MIN_VALUE;
            System.out.println(clientId + " is asking rider");
            Map<String, Map<String, Object>> driveMapInOneBlock = driver_loc.get(blockId);
            if (driveMapInOneBlock != null) {
                for (Map.Entry<String, Map<String, Object>> entry : driveMapInOneBlock.entrySet()) {
                    if (!entry.getValue().containsKey("status") || entry.getValue().get("status").equals("UNAVAILABLE")) {
                        continue;
                    }
                    Double driverLatitude = Double.valueOf((String) entry.getValue().get("latitude"));
                    Double driverLongitude = Double.valueOf((String) entry.getValue().get("longitude"));
                    Double euclideanDistance = Math.sqrt(
                            Math.pow(clientLatitude - driverLatitude, 2)
                                    + Math.pow(clientLongitude - driverLongitude, 2));
                    Double distanceScore = Math.pow(Math.exp(1), -1 * euclideanDistance);
                    Double ratingScore = Double.valueOf((String) entry.getValue().get("rating")) / 5.0;
                    Double salaryScore = 1 - Math.min(MAX_MONEY, Double.valueOf((String) entry.getValue().get("salary"))) / 100.0;
                    Double genderScore = genderPreferennce.equals("N") || entry.getValue().get("gender").equals(genderPreferennce) ? 1.0 : 0.0;
                    Double matchScore = distanceScore * 0.4 + genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;
                    if (matchScore > highestScore) {
                        highestScore = matchScore;
                        highestScoreDriverId = entry.getKey();
                    }
                }
            }

            if (highestScoreDriverId != null) {
                Map<String, Object> driverInfo = driveMapInOneBlock.get(highestScoreDriverId);
                driverInfo.put("status", "UNAVAILABLE");
                driveMapInOneBlock.put(highestScoreDriverId, driverInfo);
                System.out.println(clientId + " is going with " + highestScoreDriverId);
                message.clear();
                message.put("clientId", Integer.valueOf(clientId));
                message.put("driverId", Integer.valueOf(highestScoreDriverId));
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, message));

            }
        }
    }
}
