# Kafka Postgres Connector

## Overview

This project implements a custom Kafka connector for Apache Kafka Connect, enabling data transfer between a Kafka topic and a PostgreSQL database. It consists of a source connector that reads data from a Kafka topic and writes it to PostgreSQL, as well as a sink connector that reads data from PostgreSQL and writes it to a Kafka topic.

## Features

- **Source Connector**: Reads messages from a Kafka topic and writes them to PostgreSQL.
- **Sink Connector**: Reads rows from a PostgreSQL table and sends them to a Kafka topic.
- **Supports Avro Schema**: Integrates with Kafka's Avro serialization for structured data handling.

## Getting Started

### Prerequisites

- Java 8 or higher
- Apache Kafka
- PostgreSQL
- Maven
- Confluent Schema Registry (if using Avro)

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/YourUsername/connectors.git
   cd connectors
