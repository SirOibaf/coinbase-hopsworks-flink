# Coinbase Flink

A Flink streaming application for processing Coinbase ETH-USD ticker data and writing windowed price features to Hopsworks Feature Store.

## Prerequisites

- Java 8+
- Maven 3.x
- Access to a Hopsworks instance with API key and project

## Setup

1. **Clone the repository:**
   ```sh
   git clone https://github.com/SirOibaf/coinbase-hopsworks-flink.git
   cd coinbase-hopsworks-flink
   ```

2. **Build the project:**
   ```sh
   mvn clean package
   ```

## Configuration

- Hopsworks connection parameters (host, port, project, API key) are set in [`EthUsd.java`](src/main/java/ai/hopsworks/coinbaseflink/features/EthUsd.java).
- Feature group names and window sizes are defined in the same file.

## Running the Application

Make sure you have run the [`feature_groups.py`](setup/feature_groups.py) file to create the feature groups before running jar.

### Locally

Run the application with:
```sh
java -jar target/coinbase-flink-1.0-SNAPSHOT.jar
```

### On Flink Cluster (Kubernetes)

1. Build the Docker image:
   ```sh
   docker build -t your-repo/coinbase-flink:latest .
   ```

2. Push the image to your registry and update `coinbase-k8s.yaml` accordingly.

3. Deploy to Kubernetes:
   ```sh
   kubectl apply -f coinbase-k8s.yaml
   ```