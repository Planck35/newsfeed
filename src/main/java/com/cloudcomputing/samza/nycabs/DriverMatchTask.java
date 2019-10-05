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
import java.time.LocalTime;
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
        message.put("time", LocalTime.now().toString());
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

}
