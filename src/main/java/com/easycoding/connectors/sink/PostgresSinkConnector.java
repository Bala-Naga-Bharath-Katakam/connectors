package com.easycoding.connectors.sink;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresSinkConnector extends SinkConnector {

    @Override
    public void start(Map<String, String> props) {
        // Initialize connector
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("jdbc.url", "jdbc:postgresql://localhost:5432/yourdbname");
            config.put("topic.name", "your-kafka-topic");
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Clean up resources
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("jdbc.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "JDBC URL for PostgreSQL")
                .define("topic.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic to read from");
    }



    @Override
    public String version() {
        return "1.0.0";
    }
}

