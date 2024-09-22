package com.easycoding.connectors.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.*;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
@Slf4j
public class PostgresSourceTask extends SourceTask {

    private Connection connection;
    private String selectQuery = "SELECT id, value FROM my_table WHERE id > ? ORDER BY id ASC";
    private long lastId = 0;

    @Override
    public String version() {
        return new PostgresSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> config) throws RuntimeException {
        log.info("PostgresSourceTask starting with config: {}", config);

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
    public List<SourceRecord> poll() throws InterruptedException {
        log.info("Polling new records from Postgres starting from id {}", lastId);
        List<SourceRecord> records = new ArrayList<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery)) {
            preparedStatement.setLong(1, lastId);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String value = resultSet.getString("value");

                log.debug("Fetched record: id={}, value={}", id, value);
                records.add(new SourceRecord(null, null, "kafka-topic", Schema.INT32_SCHEMA, id, Schema.STRING_SCHEMA, value));
                lastId = id;
            }
        } catch (SQLException e) {
            log.error("Failed to query records from Postgres", e);
        }

        return records;
    }

    @Override
    public void stop() {
        log.info("PostgresSourceTask stopping.");
        try {
            if (connection != null) connection.close();
        } catch (SQLException e) {
            log.error("Failed to close Postgres connection", e);
        }
    }
}



