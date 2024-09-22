package com.easycoding.connectors.source;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresSourceConnector extends SourceConnector {

    private String jdbcUrl;
    private String topicName;

    @Override
    public void start(Map<String, String> props) {
        jdbcUrl = props.get("jdbc.url");
        topicName = props.get("topic.name");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("jdbc.url", jdbcUrl);
            config.put("topic.name", topicName);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Clean up resources
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("jdbc.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "JDBC URL for PostgreSQL")
                .define("topic.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic to write to");
    }
}

