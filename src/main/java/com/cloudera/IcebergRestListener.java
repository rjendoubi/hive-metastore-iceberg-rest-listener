package com.cloudera;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class IcebergRestListener extends MetaStoreEventListener {
    private static final Logger log = LoggerFactory
            .getLogger(IcebergRestListener.class);

    private static IcerbergRestListenerConfig config;

    private static ObjectMapper mapper = new ObjectMapper();

    private static HttpClient httpClient = HttpClient.newHttpClient();

    public IcebergRestListener(Configuration conf) {
        super(conf);

        config = new IcerbergRestListenerConfig();

        log.info(this.getClass().getName() + " created");
    }

    private void serializeEventAndSend(ListenerEvent event) {
        String jsonString = null;
        try {
            jsonString = mapper.writeValueAsString(event);
        } catch (Exception e) {
            log.error(e.toString());
        }

        // TODO: here's where JsonLogic filtering should happen
        // Should be public method for CLI-based testability

        List<URI> targetUrls = config.getTargetUrls();

        for (int i = 0; i < targetUrls.size(); i++) {
            HttpRequest req = HttpRequest.newBuilder().uri(targetUrls.get(i))
                    .header("Content-Type", "application/json")
                    // TODO: deal with specifying http method in config
                    .POST(BodyPublishers.ofString(jsonString)).build();

            try {
                httpClient.send(req, null);
            } catch (Exception e) {
                log.error(e.toString());
            }
        }
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        serializeEventAndSend(event);
    }

    @Override
    public void onAddPartition(AddPartitionEvent event) throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent event)
            throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onAlterSchemaVersion(AlterSchemaVersionEvent event)
            throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent event)
            throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onCreateTable(CreateTableEvent event) throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent event) throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onDropPartition(DropPartitionEvent event) throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onDropTable(DropTableEvent event) throws MetaException {
        serializeEventAndSend(event);
    }

    @Override
    public void onInsert(InsertEvent event) throws MetaException {
        serializeEventAndSend(event);
    }
}
