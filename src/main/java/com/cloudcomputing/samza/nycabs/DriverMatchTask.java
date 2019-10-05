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

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
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

    }

}
