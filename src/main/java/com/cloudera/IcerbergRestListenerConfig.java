package com.cloudera;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcerbergRestListenerConfig extends Configuration {
    private static final Logger log = LoggerFactory
            .getLogger(IcerbergRestListenerConfig.class);
    private static URL configUrl;

    public static final String TargetUrlVarName = "com.cloudera.hms.listeners.iceberg.rest.targets";

    public List<URI> getTargetUrls() {
        String[] urlStrings = get(TargetUrlVarName).split("|");
        ArrayList<URI> ret = new ArrayList<URI>();
        for(int i = 0; i < urlStrings.length; i++) {
            ret.add(URI.create(urlStrings[i]));
        }
        return ret;
    }

    static {
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        if (classLoader == null) {
            classLoader = IcerbergRestListenerConfig.class.getClassLoader();
        }

        String configFileName = "iceberg-rest-listener-config.xml";
        configUrl = classLoader.getResource(configFileName);

        if (configUrl == null) {
            log.warn(String.format("Couldn't find config file '%s'",
                    configFileName));

            configFileName = "hive-site.xml";
            configUrl = classLoader.getResource(configFileName);
            if (configUrl == null) {
                log.error(String.format("Couldn't find config file '%s'",
                        configFileName));
            }
        }
    }

    public IcerbergRestListenerConfig() {
        super(false);
        if (configUrl != null) {
            addResource(configUrl);
        } else {
            // TODO: Should this throw instead or just logging?
            // What would that do to a running HMS instance?
            log.error("Could not load config");
        }
    }
}
