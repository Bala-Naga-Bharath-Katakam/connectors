package com.easycoding.connectors.sink;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PostgresSinkTask extends SinkTask {

    private Connection connection;
    private String insertQuery = "INSERT INTO my_table (id, value) VALUES (?, ?)";

    @Override
    public String version() {
        return new PostgresSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> config) {
        log.info("PostgresSinkTask starting with config: {}", config);

        try {
            String url = config.get("postgres.url");
            String username = config.get("postgres.username");
            String password = config.get("postgres.password");
            connection = DriverManager.getConnection(url, username, password);
            log.info("Connected to Postgres at {}", url);
        } catch (SQLException e) {
            log.error("Failed to connect to Postgres", e);
            throw new ConnectException("Failed to connect to Postgres", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Received {} records for processing.", records.size());

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
            for (SinkRecord record : records) {
                log.debug("Processing record: {}", record);
                preparedStatement.setInt(1, (Integer) record.key());
                preparedStatement.setString(2, (String) record.value());
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            log.error("Failed to insert records into Postgres", e);
        }
    }

    @Override
    public void stop() {
        log.info("PostgresSinkTask stopping.");
        try {
            if (connection != null) connection.close();
        } catch (SQLException e) {
            log.error("Failed to close Postgres connection", e);
        }
    }
}

