# AI-Assisted Message Routing in Pub/Sub Systems

## 💡 Project Overview

This repository contains the implementation of a lightweight **MQTT broker** that leverages **Large Language Models (LLMs)** and a **Rule-Based Engine** for **AI-assisted message routing**. The core goal is to classify incoming streaming messages (simulated using real-world energy consumption data) by priority and route them efficiently to the correct subscriber pools.

This system demonstrates a practical, end-to-end MLOps solution for enhancing traditional Pub/Sub architectures, improving **message delivery efficiency**, and providing a critical alert mechanism for enterprise or smart energy management systems.

---

## 🚀 Key Features and Components

### Smart Broker Core (`broker.py`)
* **LLM Integration:** Utilizes an LLM (via API) to analyze complex message content and dynamically assign a priority or routing policy, moving beyond static rules.
* **Dynamic Scaling:** Implements logic to dynamically scale up or down the number of active **normal subscribers** based on current message load and queue length, ensuring traffic balancing.
* **Performance Metrics:** Tracks and logs key metrics like message processing times, routing accuracy, and cache hits for system evaluation.

### Rule Engine (`rule_manager.py`)
* **Flexible Policy Management:** Manages predefined and static routing policies based on message payload content, allowing for both basic and complex filtering conditions (e.g., checking multiple fields or using wildcards like `zones.*.temperature`).
* **Hybrid Routing:** The final routing decision is based on a hybrid approach, using the **Rule Engine** as a fast fallback and the **LLM** for ambiguous or critical messages.

### End-to-End Messaging
* **Data Publisher (`publisher.py`):** Reads energy consumption data from a CSV and publishes structured JSON messages to the broker at a configurable interval, simulating a real-time data stream.
* **Subscriber Pools:**
    * **Normal Subscriber (`normal_subscriber.py`):** Handles standard, low-priority messages.
    * **Red Alert Subscriber (`red_subscriber.py`):** Dedicated to handling critical, high-priority messages requiring immediate attention.

### Testing and Evaluation (`test.py`)
* **Comparative Testing:** Includes a robust test harness to measure **latency**, **throughput**, and **routing accuracy** by comparing the **'ai'** mode against a **'traditional'** (rule-only) mode.
* **Visualization:** Generates reports and plots (histograms/scatter plots) to visually demonstrate the performance improvements enabled by the AI component.

---

## ⚙️ Setup and Run Instructions

### Prerequisites

1.  **MQTT Broker:** Ensure a local MQTT broker (like Mosquitto) is running on `localhost:1883`.
2.  **Dependencies:** Install required Python packages.

    ```bash
    pip install paho-mqtt pandas openai colorama matplotlib
    ```
3.  **Data:** The `publisher.py` requires an `energydata_complete.csv` file (or similar structured data) to simulate the message stream.
4.  **API Key:** Update the `groq_key` parameter in `broker.py` with your actual Groq or OpenAI key for LLM functionality.

### Running the System

1.  **Start the Broker (Server):**
    ```bash
    python broker.py 
    ```
    *(Use the `--disable-ai` flag to run in traditional rule-only mode for comparison: `python broker.py --disable-ai`)*

2.  **Start Critical Subscriber (Run in a Separate Terminal):**
    ```bash
    python red_subscriber.py
    ```
    *(The broker manages the lifecycle of the normal subscribers automatically.)*

3.  **Start the Publisher (Simulated Data Stream):**
    ```bash
    python publisher.py
    ```

### Running Performance Tests

Execute the automated test script to generate performance metrics and comparisons:

```bash
python test.py
