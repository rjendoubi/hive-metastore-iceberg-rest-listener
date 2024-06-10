package com.cloudera;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Exploratory unit tests for code outside of this package's core
 * responsibilities.
 */
public class AssumptionTests {
    @Test
    public void insertEventShouldSerializeToJsonString() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AddPartitionEvent event = null;
        List<Partition> partitions = new ArrayList<>();
        Partition p = new Partition();
        p.setValues(Arrays.asList("1", "part1"));
        partitions.add(p);
        try {
            event = new AddPartitionEvent(new Table(),
                    partitions,
                    true,
                    null);
        } catch (Exception e) {
            throw e;
        }

        String jsonString = null;
        try {
            jsonString = mapper.writeValueAsString(event);
        } catch (Exception e) {
            fail(e.toString());
        }

        assertEquals("Generated JSON from InsertEvent", "{\"foo\":\"bar\"}",
                jsonString);

    }
}
