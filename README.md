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
<img width="888" height="777" alt="image" src="https://github.com/user-attachments/assets/4d6be2c8-3d6c-46a3-80d5-e2b7415d227a" />

---


### Sensors Threshold and Usage
<img width="1057" height="1144" alt="Screenshot 2026-06-06 at 5 11 30 AM" src="https://github.com/user-attachments/assets/21331e1f-fff0-427c-ae76-973bc01e34e7" />

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
<img width="402" height="242" alt="image" src="https://github.com/user-attachments/assets/7e5f36fa-7038-4235-8c9e-6e13529cce1d" />

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

<img width="444" height="464" alt="image" src="https://github.com/user-attachments/assets/98078408-8358-422d-bea9-d296ab1ac41e" />

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
<img width="1430" height="2227" alt="Screenshot 2026-06-13 at 10 45 01 PM" src="https://github.com/user-attachments/assets/4703d58e-7399-4ab4-9f94-d8bd4cde2517" />


---

# Analytics Comparison

The project compares different approaches for analyzing environmental sensor streams.

Compared techniques:

* Original Time-Series Data
* Rolling Average
* Minimum Values
* Maximum Values
* IoTDB Aggregation
---

# Future Enhancements

The current implementation uses CO₂ concentration as the primary occupancy indicator.

Future improvements may include:

* PIR (Passive Infrared) Sensors
* mmWave Radar Sensors
* Ultrasonic Sensors
* Machine Learning-Based Occupancy Detection
* Automated Alert Systems

A multi-sensor approach can significantly improve detection accuracy and reduce false occupancy events.

---

# Technology Stack

## Embedded Systems

* ESP32
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
