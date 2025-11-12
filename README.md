# LogFlowIDS

[![License: GPL-3.0](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://github.com/fabrizio-07/LogFlowIDS/blob/main/LICENSE)

**LogFlowIDS** is a lightweight, containerized, real-time intrusion detection system (IDS) for MacOS. It's designed to run efficiently on low-resource hardware (e.g., older 2-core, 8GB RAM MacBooks).

It works by streaming macOS system logs into a data pipeline, processing them with both rule-based and machine-learning-based detection, and making the results available for analysis in a visual dashboard.

---

## Table of Contents

* [Features](#-features)
* [Architecture](#-architecture)
* [Detection Logic](#-detection-logic)
    * [Rule-Based Detection](#rule-based-detection)
    * [Machine Learning Detection](#machine-learning-detection)
* [Prerequisites](#-prerequisites)
* [Getting Started](#-getting-started)
    * [1. Clone the Repository](#1-clone-the-repository)
    * [2. Start the Pipeline](#2-start-the-pipeline)
    * [3. Stream macOS Logs](#3-stream-macos-logs)
    * [4. Access Kibana](#4-access-kibana)
* [Project Structure](#-project-structure)
* [Configuration](#-configuration)
* [Contributing](#-contributing)
* [License](#-license)

---

## Features

* **Real-Time Processing:** Uses Spark Streaming to analyze logs as they arrive.
* **Hybrid Detection:** Combines a fast, rule-based engine with a machine learning (Isolation Forest) model for anomaly detection.
* **Lightweight & Containerized:** All components run in Docker, managed by a single `docker-compose` file. Optimized for low-resource machines.
* **Scalable & Resilient:** Uses Kafka as a durable message broker between ingestion and processing.
* **Easy to Query:** Enriched logs are stored in Elasticsearch and explorable in Kibana.

## Architecture

The project consists of a five-stage pipeline, with each service running in its own Docker container:



1.  **Ingestion (Fluentd):** A `fluentd` service listens on port `42069` for incoming macOS system logs.
2.  **Message Broker (Kafka):** Fluentd forwards all logs to the `macos_logs` topic in Kafka. This provides a resilient buffer, ensuring logs are not lost if the processing layer is down.
3.  **Processing (Spark):** A Spark Streaming job (`process_logs.py`) consumes from the Kafka topic. This is the core of the IDS, where all detection logic is applied.
4.  **Storage (Elasticsearch):** Spark writes the enriched logs—now with suspicion flags and rule-names—to an Elasticsearch index named `logs-enriched`.
5.  **Visualization (Kibana):** Kibana connects to Elasticsearch, allowing you to query, analyze, and build dashboards based on the detected threats.

## Detection Logic

The logic resides in `spark/src/process_logs.py`. Each log entry is analyzed by two independent detection engines.

### Rule-Based Detection

A set of 10 hard-coded rules flags common suspicious activities on macOS. If a log matches any rule, it is flagged as `rule_is_suspicious = 1` and annotated with the `rule_name`.

* **Rule 1:** `LaunchAgent/Daemon loading`
* **Rule 2:** `Crontab modification`
* **Rule 3:** `Process injection`
* **Rule 4:** `Gatekeeper disabled`
* **Rule 5:** `Quarantine attribute cleared`
* **Rule 6:** `Log clearing/tampering`
* **Rule 7:** `AppleScript execution`
* **Rule 8:** `Obfuscated shell command`
* **Rule 9:** `Netcat usage`
* **Rule 10:** `System profiling`

### Machine Learning Detection

In parallel, a machine learning model analyzes the log content for anomalies that don't fit a known rule.

* **Feature Engineering:** The `eventMessage` and `processImagePath` fields are combined into a single text string. A pre-trained `TF-IDF Vectorizer` (`tfidf_model.joblib`) converts this text into a numerical vector.
* **Model:** A pre-trained `Isolation Forest` model (`isolation_forest_model.joblib`) predicts whether the log's features are "normal" (score `1`) or an "anomaly" (score `-1`).
* **Flagging:** Logs flagged as anomalies are marked `ml_is_suspicious = 1`.

A final flag, `is_suspicious`, is set to `1` if *at least* one of the rule-based or ML-based engine flags the log.

## Prerequisites

Before you begin, ensure you have the following installed on your host machine:

* **Docker Desktop:** [Install Docker](https://docs.docker.com/get-docker/) (Ensure it has at least 4GB of RAM allocated in its settings).
* **Git:** [Install Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

## Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/fabrizio-07/LogFlowIDS
cd LogFlowIDS   #move into the project folder
```
### 2. Build the Custom Images

First, build the custom `fluentd` and `spark` images from their Dockerfiles. This step bundles your Python script and ML models into the `spark` image, and the `fluentd` configuration into its image.
```bash
docker-compose build
```
### 3. Start the Pipeline

Now that the images are built, the aim of the next command, is to start all 6 services. They will launch in the correct order, with healthchecks ensuring `kafka` and `elasticsearch` are ready before `spark` starts.
```bash
docker-compose up -d
```
You can monitor the logs of all services:
```bash
docker-compose logs -f
```
Or check a specific service, like `spark`, to confirm the models loaded:
```bash
docker-compose logs -f spark
```
You should see output from Spark confirming "*ML models loaded successfully.*"

### 3. Stream macOS Logs

The pipeline is now running and waiting for data. On your **MacOS host** (not in a container), open a new terminal window and run the following command:
```bash
log stream --level default --style ndjson --color none --predicate '\''NOT (subsystem == "com.apple.chrono" OR subsystem == "com.apple.icloud.SPFinder" OR subsystem == "com.apple.icloud.SPOwner" OR subsystem == "com.apple.bluetooth" OR subsystem == "com.apple.rapport" OR subsystem == "com.apple.Multitouch" OR subsystem == "com.apple.PlugInKit")'\'' | grep '^{' | nc localhost 42069 &
```
This command:
* Starts streaming system logs (`log stream`) as `ndjson`.
* Applies a `predicate` to filter out common, noisy subsystems.
* Removes logs flagged as `INFO` and `DEBUG` level.
* Pipes the filtered, JSON-like logs to `netcat` (`nc`), which forwards them to Fluentd on port `42069`.

### 4. Access Kibana

1.  Open your web browser and navigate to `http://localhost:5601`.
2.  Kibana may take a minute to initialize. Once loaded, you'll need to tell it about your data.
3.  Go to **Management > Stack Management > Saved Objects**. Then click on the *Import*, written on top-right corner.
4.  Upload the `kibana/kibana_dashboard.ndjson` file.
5.  Click on the object `LogFlowIDS`, listed as a *dashboard* type.

You can now see your enriched logs streaming.



---

## Configuration

* **Log Ingestion:** It's possible to use other way to ingest logs, such as passing a `.jsonl` file, containing **MacOS** logs.
* **Detection Rules:** To add or change detection rules, edit the `when(...)` clauses in `spark/src/process_logs.py`.
* **Kafka Topic:** The topic `macos_logs` is auto-created by the Kafka container.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request. For any questions, you can find me as [Fabrizio](https://t.me/Avaja_mbare) on *Telegram*

## 📜 License

This project is licensed under the GPL-3.0 License. See the [LICENSE](LICENSE) file for details.