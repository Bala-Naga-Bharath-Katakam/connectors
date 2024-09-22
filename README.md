# Kafka Postgres Connector

This project demonstrates custom **Kafka Connectors** for PostgreSQL as both a **Source Connector** (pushing data from PostgreSQL to a Kafka topic) and a **Sink Connector** (reading data from a Kafka topic and inserting it into PostgreSQL).

## Table of Contents

- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Building the Project](#building-the-project)
- [Deploying the Custom Kafka Connectors](#deploying-the-custom-kafka-connectors)
- [Kafka Connect Configuration](#kafka-connect-configuration)
- [Connector Configuration](#connector-configuration)
   - [Source Connector](#source-connector)
   - [Sink Connector](#sink-connector)
- [Starting Kafka Connect](#starting-kafka-connect)
   - [Standalone Mode](#standalone-mode)
   - [Distributed Mode](#distributed-mode)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Requirements

- **Apache Kafka** (with Kafka Connect)
- **PostgreSQL** (with a sample database)
- **Maven** (for building the project)
- **JDK 8+**


## Building the Project

1. Clone the repository and navigate to the project directory:

   ```bash
   git clone <repository-url>
   cd kafka-postgres-connector
   
2. Build the project using Maven. This will package your code and dependencies into a single JAR file:
    mvn clean package

3. Summary of Method Triggers
   Connector Lifecycle:
   start() (Connector) → taskConfigs() → start() (each Task) → put() (Sink Task) or poll() (Source Task)
   stop() (each Task) → stop() (Connector)