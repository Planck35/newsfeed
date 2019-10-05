package com.cloudcomputing.samza.nycabs;

import org.apache.samza.system.SystemStream;

public class DriverMatchConfig {
    public static final SystemStream MATCH_STREAM = new SystemStream("kafka", "match-stream");
}
