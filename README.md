# Human Detection Without Camera Using ESP32 and Environmental Sensors

A privacy-preserving IoT system that detects human presence without using cameras by combining environmental sensing, real-time stream processing, event detection, and data visualization.

---

## Overview

This project presents an end-to-end IoT-based human presence detection system developed using ESP32, environmental sensors, MQTT, Apache Kafka, Apache Flink, Grafana, and Apache IoTDB.

Unlike traditional occupancy monitoring systems that rely on cameras, this system utilizes environmental parameters such as carbon dioxide concentration, temperature, humidity, illuminance, and atmospheric pressure to estimate human presence while maintaining user privacy.

The project demonstrates the complete lifecycle of IoT data:

1. Environmental Data Acquisition
2. Real-Time Communication
3. Stream Analytics and Event Processing
4. Occupancy Detection
5. Real-Time Visualization

---

## System Architecture

![System Architecture](docs/system_architecture.png)

The workflow consists of:

```text
ESP32 + Sensors
        │
        ▼
      MQTT
        │
        ▼
 Apache Kafka
        │
        ▼
 Apache Flink
        │
        ▼
 Occupancy Detection
        │
        ▼
 Grafana / IoTDB
```

---

## Hardware Platform

The system is built around an ESP32 microcontroller and multiple I2C-based environmental sensors.

| Component  | Purpose                              |
| ---------- | ------------------------------------ |
| ESP32      | Main controller                      |
| SCD41      | CO₂, Temperature, Humidity           |
| DPS310     | Air Pressure, Temperature            |
| BH1750     | Illuminance Measurement              |
| RPR-0521RS | Illuminance and Infrared Measurement |

### Hardware Setup

![Hardware Setup](docs/hardware_setup.png)

---

# Project Objectives

The project was developed through three major implementation phases that collectively form the final human detection system.

## Objective 1 — Environmental Data Acquisition

Develop a reliable environmental sensing platform capable of periodically acquiring data from multiple sensors connected to an ESP32.

Implemented capabilities:

* CO₂ monitoring
* Temperature monitoring
* Humidity monitoring
* Air pressure monitoring
* Illuminance monitoring
* Infrared illumination monitoring
* Multi-sensor integration
* CSV data logging

### Sample Sensor Output

![Sensor Output](docs/sensor_output.png)

---

## Objective 2 — Real-Time Data Communication

Design a communication infrastructure capable of transmitting environmental data between distributed components.

Implemented capabilities:

* MQTT Publish/Subscribe Communication
* Kafka Integration
* Rolling Average Computation
* Threshold Detection
* Event-Based Communication
* LED Status Control

### Communication Workflow

![Communication Workflow](docs/communication_workflow.png)

---

## Objective 3 — Stream Analytics and Human Presence Detection

Develop a real-time analytics platform capable of processing sensor streams and detecting room occupancy.

Implemented capabilities:

* Apache Flink Stream Processing
* Sliding Window Analytics
* Minimum Value Calculation
* Maximum Value Calculation
* Average Value Calculation
* Occupancy Detection
* Event Publishing
* Dashboard Visualization

### Human Presence Detection

The current implementation uses CO₂ concentration as the primary indicator of occupancy.

When environmental conditions satisfy predefined thresholds:

* Occupancy events are generated
* Kafka publishes detection results
* Grafana visualizes occupancy status
* ESP32 LED indicators provide visual confirmation

### Human Presence Detection Result

![Human Presence Detection](docs/human_presence_detection.png)

---

# Data Visualization

Grafana dashboards were developed to monitor both raw and processed sensor data in real time.

Visualized data includes:

* Environmental Sensor Data
* Rolling Average Results
* Stream Analytics Results
* Occupancy Events
* IoTDB Aggregation Results

### Grafana Dashboard

![Grafana Dashboard](docs/grafana_dashboard.png)

---

# Analytics Comparison

The project compares different approaches for analyzing environmental sensor streams.

Compared techniques:

* Original Time-Series Data
* Rolling Average
* Minimum Values
* Maximum Values
* IoTDB Aggregation

### Comparison Dashboard

![Analytics Comparison](docs/analytics_comparison.png)

The comparison demonstrates how stream-processing analytics and database-level aggregation can produce slightly different visual representations while using the same source data.

---

# Future Enhancements

The current implementation uses CO₂ concentration as the primary occupancy indicator.

Future improvements may include:

* PIR (Passive Infrared) Sensors
* mmWave Radar Sensors
* Ultrasonic Sensors
* Sensor Fusion Techniques
* Machine Learning-Based Occupancy Detection
* Cloud Deployment
* Automated Alert Systems

A multi-sensor approach can significantly improve detection accuracy and reduce false occupancy events.

---

# Technology Stack

## Embedded Systems

* ESP32
* MicroPython
* ESP-IDF

## Messaging

* MQTT
* Apache Kafka

## Stream Processing

* Apache Flink

## Time-Series Database

* Apache IoTDB

## Visualization

* Grafana

## Programming Languages

* Python
* MicroPython

---

# Repository Structure

```text
Human-Detection-Without-Camera-Using-ESP32-and-Environmental-Sensors
│
├── docs/
│   ├── hardware_setup.png
│   ├── system_architecture.png
│   ├── sensor_output.png
│   ├── communication_workflow.png
│   ├── human_presence_detection.png
│   ├── grafana_dashboard.png
│   └── analytics_comparison.png
│
├── firmware/
│   ├── sensor_acquisition/
│   ├── mqtt_publisher/
│   └── mqtt_subscriber/
│
├── analytics/
│   ├── kafka_processing/
│   ├── flink_stream_analytics/
│   └── occupancy_detection/
│
├── dashboards/
│
├── sample_data/
│
└── README.md
```

---

# Security Notice

To protect infrastructure and personal information, all sensitive identifiers have been removed from this repository.

Examples:

```python
BROKER_IP = "xxx.xxx.xxx.xxx"
BROKER_PORT = XXXX

KAFKA_SERVER = "xxx.xxx.xxx.xxx:XXXX"

STUDENT_ID = "sXXXXXXX"
```

Replace these values with your own deployment configuration before use.

---

# License

This repository is provided for educational and research purposes.
