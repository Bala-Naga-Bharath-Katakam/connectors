package com.easycoding.connectors.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

public class PostgresSourceTask extends SourceTask {

    private String jdbcUrl;
    private String topicName;

    @Override
    public void start(Map<String, String> props) {
        jdbcUrl = props.get("jdbc.url");
        topicName = props.get("topic.name");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Placeholder for reading from Kafka and writing to PostgreSQL
        return List.of(); // Implement actual polling logic here
    }

    private void writeToPostgres(String message) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             PreparedStatement pstmt = connection.prepareStatement("INSERT INTO your_table (column) VALUES (?)")) {
            pstmt.setString(1, message); // Adjust according to your schema
            pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        // Clean up resources if needed
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}

