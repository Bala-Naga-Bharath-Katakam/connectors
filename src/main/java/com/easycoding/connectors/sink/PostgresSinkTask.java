package com.easycoding.connectors.sink;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PostgresSinkTask extends SinkTask {
    private Connection connection;

    @Override
    public void start(Map<String, String> props) {
        try {
            connection = DriverManager.getConnection(props.get("db.url"), props.get("db.user"), props.get("db.password"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to establish PostgreSQL connection", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            for (SinkRecord record : records) {
                String jsonValue = (String) record.value();
                // Parse JSON string into a Map using Jackson
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> valueMap = objectMapper.readValue(jsonValue, Map.class);
                PreparedStatement preparedStatement = connection.prepareStatement(
                        "INSERT INTO users (name, email) VALUES (?, ?)"
                );
                preparedStatement.setString(1, (String) valueMap.get("name"));
                preparedStatement.setString(2, (String) valueMap.get("email"));
                preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error writing to PostgreSQL", e);
        }
    }

    @Override
    public void stop() {
        try {
            if (connection != null) connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}
