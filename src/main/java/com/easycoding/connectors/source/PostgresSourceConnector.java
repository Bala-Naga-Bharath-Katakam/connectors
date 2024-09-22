package com.easycoding.connectors.source;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class PostgresSourceConnector extends SourceConnector {

    private Map<String, String> config;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> config) {
        this.config = config;
        log.info("PostgresSourceConnector started with config: {}", config);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating task configurations for {} tasks.", maxTasks);
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping PostgresSourceConnector.");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("postgres.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Postgres database URL")
                .define("postgres.username", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Postgres database username")
                .define("postgres.password", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Postgres database password");
    }
}


