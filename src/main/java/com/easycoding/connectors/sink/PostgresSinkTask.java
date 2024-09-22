package com.easycoding.connectors.sink;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class PostgresSinkTask extends SinkTask {

    private String jdbcUrl;
    private String topicName;
    private Connection connection;


    @Override
    public void start(Map<String, String> map) {
        jdbcUrl = map.get("jdbc.url");
        topicName = map.get("topic.name");

        try {
            connection = DriverManager.getConnection(jdbcUrl);
            connection.setAutoCommit(false); // Use transaction
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to PostgreSQL", e);
        }

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        String insertSQL = "INSERT INTO your_table (column1, column2) VALUES (?, ?)"; // Adjust to your schema

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            for (SinkRecord record : records) {
                // Extract data from the record
                Object value1 = record.value(); // Adjust based on your data structure
                Object value2 = record.key(); // Adjust as necessary

                // Set values in prepared statement
                preparedStatement.setObject(1, value1);
                preparedStatement.setObject(2, value2);

                preparedStatement.addBatch(); // Add to batch
            }
            preparedStatement.executeBatch(); // Execute batch insert
            connection.commit(); // Commit transaction
        } catch (SQLException e) {
            throw new RuntimeException("Error writing to PostgreSQL", e);
        }
    }

    @Override
    public void stop() {
        try {
            if (connection != null) {
                connection.close(); // Close the connection
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public String version() {
        return "1.0.0";
    }
}
