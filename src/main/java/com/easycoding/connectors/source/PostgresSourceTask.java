package com.easycoding.connectors.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class PostgresSourceTask extends SourceTask {
    private Connection connection;
    private String topic;

    @Override
    public void start(Map<String, String> props) {
        topic = props.get("topic");
        try {
            connection = DriverManager.getConnection(props.get("db.url"), props.get("db.user"), props.get("db.password"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to establish PostgreSQL connection", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT id, name, email FROM users WHERE processed = false LIMIT 10");

            while (rs.next()) {
                Map<String, Object> sourcePartition = Collections.singletonMap("db", "postgres");
                Map<String, Object> sourceOffset = Collections.singletonMap("lastId", rs.getInt("id"));

                Map<String, Object> value = new HashMap<>();
                value.put("id", rs.getInt("id"));
                value.put("name", rs.getString("name"));
                value.put("email", rs.getString("email"));

                records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, value.toString()));
            }

            // Mark records as processed
            statement.executeUpdate("UPDATE users SET processed = true WHERE processed = false LIMIT 10");
        } catch (Exception e) {
            throw new RuntimeException("Error polling data from PostgreSQL", e);
        }
        return records;
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


