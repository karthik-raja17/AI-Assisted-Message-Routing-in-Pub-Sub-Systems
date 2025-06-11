# File: test.py

import time
import paho.mqtt.client as mqtt
import json
import random
from datetime import datetime
import statistics
import matplotlib.pyplot as plt
import pandas as pd
import subprocess
import sys
import os
import uuid
import threading
from collections import deque

# --- Configuration ---
TEST_DURATION = 20  # seconds per test case (reduced for lighter test)
PUBLISH_INTERVAL = 1.5 # seconds between messages (increased for lighter load)
WARMUP_TIME = 5     # seconds, allow broker to initialize
COOLDOWN_TIME = 5   # seconds, allow messages to propagate after publishing
TEST_MODES = ['ai', 'traditional'] # 'ai' means broker runs with AI, 'traditional' means broker runs with --disable-ai
RESULTS_DIR = 'test_results' # Renamed to avoid confusion with broker's log file

# Test cases with expected routing behavior and typical data ranges
# These ranges should ideally align with what the RuleManager and AI are trained on
TEST_CASES = [
    {"name": "Normal_Load", "power_range": (100, 250), "temp_range": (18, 24), "expected_route": "normal"},
    {"name": "High_Power_Anomaly", "power_range": (450, 600), "temp_range": (20, 25), "expected_route": "critical"},
    {"name": "High_Temp_Anomaly", "power_range": (150, 300), "temp_range": (30, 35), "expected_route": "critical"},
    {"name": "Borderline_Normal", "power_range": (250, 300), "temp_range": (24, 26), "expected_route": "normal"},
    {"name": "Borderline_Critical_Power", "power_range": (400, 440), "temp_range": (20, 25), "expected_route": "critical"}
]

class TestSystem:
    def __init__(self, mode):
        self.mode = mode
        self.broker_process = None
        self.publisher_client = None
        self.monitor_client = None
        
        # Data structures for metrics collection
        self.received_messages_queue = deque() # Stores (message_id, actual_route_type, receive_time)
        self.message_send_times = {}        # Stores {message_id: publish_time} for latency calculation
        self.monitor_lock = threading.Lock() # Protects access to shared data
        
    def _on_monitor_message(self, client, userdata, msg):
        """Callback for messages received by the monitoring client."""
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            message_id = payload.get('message_id')
            
            # Determine the actual route type based on the topic
            actual_route_type = "unknown"
            if "energy/normal" in msg.topic:
                actual_route_type = "normal"
            elif "energy/critical" in msg.topic:
                actual_route_type = "critical"

            receive_time = time.time()
            
            with self.monitor_lock:
                if message_id in self.message_send_times:
                    send_time = self.message_send_times.pop(message_id) # Remove to prevent duplicate processing
                    latency = receive_time - send_time
                    self.received_messages_queue.append({
                        "message_id": message_id,
                        "actual_route": actual_route_type,
                        "latency": latency,
                        "expected_route": payload.get('expected_route') # Get expected from payload
                    })
                # If message_id not in send_times, it might be a message from previous test or not initiated by this test
                else:
                    # self.logger.warning(f"Received message {message_id} not in send_times. Topic: {msg.topic}")
                    pass # Silently ignore messages not tracked

        except json.JSONDecodeError:
            print(f"Monitor: Received non-JSON message on {msg.topic}")
        except Exception as e:
            print(f"Monitor: Error processing message on {msg.topic}: {str(e)}")

    def start(self):
        """Start the test system components: broker and MQTT clients."""
        print(f"Starting broker in {self.mode} mode...")
        broker_cmd = [sys.executable, "broker.py"]
        if self.mode == 'traditional':
            broker_cmd.append("--disable-ai") # Pass flag to disable AI in broker
        
        self.broker_process = subprocess.Popen(
            broker_cmd,
            stdout=subprocess.PIPE, # Capture broker's stdout
            stderr=subprocess.PIPE  # Capture broker's stderr
        )
        # Read broker startup logs to confirm readiness (optional, but good practice)
        # For simplicity in this script, we rely on a fixed sleep
        time.sleep(WARMUP_TIME) 
        print(f"Broker started. Waiting {WARMUP_TIME} seconds for initialization...")

        # Initialize and connect MQTT publisher client
        self.publisher_client = mqtt.Client(f"TestPublisher_{self.mode}")
        self.publisher_client.connect("localhost", 1883, 60)
        self.publisher_client.loop_start() # Start non-blocking loop
        print("Publisher MQTT client connected.")

        # Initialize and connect MQTT monitor client (subscribes to broker outputs)
        self.monitor_client = mqtt.Client(f"TestMonitor_{self.mode}")
        self.monitor_client.on_message = self._on_monitor_message
        self.monitor_client.connect("localhost", 1883, 60)
        self.monitor_client.subscribe("energy/normal/#") # Subscribe to all normal routes
        self.monitor_client.subscribe("energy/critical/#") # Subscribe to all critical routes
        self.monitor_client.loop_start() # Start non-blocking loop
        print("Monitor MQTT client connected and subscribed to broker outputs.")
        time.sleep(1) # Give subscriptions a moment to establish

    def stop(self):
        """Stop the test system components."""
        if self.publisher_client:
            self.publisher_client.loop_stop()
            self.publisher_client.disconnect()
        if self.monitor_client:
            self.monitor_client.loop_stop()
            self.monitor_client.disconnect()
        if self.broker_process:
            self.broker_process.terminate()
            self.broker_process.wait(timeout=5)
            # Capture any remaining output from broker for debugging if needed
            broker_stdout, broker_stderr = self.broker_process.communicate()
            if broker_stdout:
                print(f"\n--- Broker STDOUT ({self.mode} mode) ---\n{broker_stdout.decode()}")
            if broker_stderr:
                print(f"\n--- Broker STDERR ({self.mode} mode) ---\n{broker_stderr.decode()}")
            print(f"Broker ({self.mode} mode) terminated.")
        print("Test system stopped.")
    
    def run_test_case(self, test_case):
        """Run a single test case and collect metrics."""
        print(f"  Running {test_case['name']} (Mode: {self.mode})...", end=' ', flush=True)
        
        # Clear previous test data
        self.received_messages_queue.clear()
        self.message_send_times.clear()
        
        messages_sent_count = 0
        test_start_time = time.time()
        
        while time.time() - test_start_time < TEST_DURATION:
            message = self._generate_message(test_case)
            message_id = message['message_id']
            
            with self.monitor_lock:
                self.message_send_times[message_id] = time.time() # Store send time before publishing
            
            self.publisher_client.publish("building/energy", json.dumps(message))
            messages_sent_count += 1
            time.sleep(PUBLISH_INTERVAL) # Control message workload

        time.sleep(COOLDOWN_TIME) # Allow messages to be fully processed and received
        
        # --- Analyze collected metrics ---
        total_latency = 0
        correct_routes = 0
        incorrect_routes = 0
        total_received = 0
        
        with self.monitor_lock: # Acquire lock before accessing received_messages_queue
            for received_msg in self.received_messages_queue:
                total_received += 1
                total_latency += received_msg['latency']
                
                # Check routing accuracy
                if received_msg['actual_route'] == received_msg['expected_route']:
                    correct_routes += 1
                else:
                    incorrect_routes += 1
                    # print(f"MISROUTE: Msg {received_msg['message_id']} - Expected: {received_msg['expected_route']}, Actual: {received_msg['actual_route']}") # For detailed debugging
        
        avg_latency_sec = total_latency / total_received if total_received > 0 else 0
        throughput_msg_per_sec = messages_sent_count / TEST_DURATION if TEST_DURATION > 0 else 0
        routing_accuracy = correct_routes / total_received if total_received > 0 else 1.0 # 1.0 if no messages received to avoid ZeroDivisionError
        
        print(f"Done. Sent: {messages_sent_count}, Received: {total_received}")
        
        return {
            "test_case": test_case["name"],
            "mode": self.mode,
            "duration_seconds": TEST_DURATION,
            "messages_sent": messages_sent_count,
            "messages_received": total_received,
            "throughput_msg_per_sec": throughput_msg_per_sec,
            "avg_latency_sec": avg_latency_sec,
            "routing_accuracy": routing_accuracy,
            "correct_routes": correct_routes,
            "incorrect_routes": incorrect_routes,
            "subscriber_load": { # These are still simulated, as actual load metrics aren't exposed by broker
                "normal": random.uniform(0.3, 0.8),
                "critical": random.uniform(0.1, 0.5)
            }
        }
            
    def _generate_message(self, test_case):
        """Generate a test message with a unique ID and expected route."""
        power = random.randint(*test_case["power_range"])
        temp = random.uniform(*test_case["temp_range"])
        
        # Ensure 'expected_route' is included in the published payload for verification
        return {
            "message_id": str(uuid.uuid4()), # Unique ID for tracking
            "energy": {
                "total": power,
                "lights": int(power * 0.2),
                "hvac": int(power * 0.4),
                "equipment": int(power * 0.3) # Added equipment for more complete data
            },
            "zones": {
                "zone_1": {"temperature": temp, "humidity": random.uniform(40, 60)}, # Added humidity
                "zone_2": {"temperature": random.uniform(18, 25), "humidity": random.uniform(40, 60)},
                "zone_3": {"temperature": random.uniform(18, 25), "humidity": random.uniform(40, 60)}
            },
            "expected_route": test_case["expected_route"], # Crucial for accuracy check
            "timestamp": datetime.now().isoformat()
        }

# --- Reporting and Visualization Functions ---

def ensure_results_dir():
    """Ensure results directory exists."""
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)

def save_results(results, filename):
    """Save results to JSON file."""
    ensure_results_dir()
    filepath = os.path.join(RESULTS_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Saved results to {filepath}")

def generate_comparison_report(all_results):
    """Generate a detailed textual comparison report."""
    report = []
    report.append("="*80)
    report.append("AI vs. Traditional Routing Performance Report")
    report.append("="*80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report.append(f"Test Duration per Case: {TEST_DURATION} seconds")
    report.append(f"Publish Interval: {PUBLISH_INTERVAL} seconds\n")

    df = pd.DataFrame(all_results)
    
    # Overall Summary
    report.append("Overall Performance Summary:")
    report.append("-" * 30)
    summary_metrics = ['throughput_msg_per_sec', 'avg_latency_sec', 'routing_accuracy']
    
    for metric in summary_metrics:
        ai_mean = df[df['mode'] == 'ai'][metric].mean()
        traditional_mean = df[df['mode'] == 'traditional'][metric].mean()
        
        report.append(f"\nMetric: {metric.replace('_', ' ').title()}")
        report.append(f"  AI-assisted Average: {ai_mean:.3f}")
        report.append(f"  Traditional Average: {traditional_mean:.3f}")
        
        if traditional_mean != 0:
            if 'latency' in metric:
                improvement = (traditional_mean - ai_mean) / traditional_mean * 100
                report.append(f"  AI Improvement (lower is better): {improvement:.2f}% {'(Faster)' if improvement > 0 else ('(Slower)' if improvement < 0 else '')}")
            else:
                improvement = (ai_mean - traditional_mean) / traditional_mean * 100
                report.append(f"  AI Improvement (higher is better): {improvement:.2f}% {'(Better)' if improvement > 0 else ('(Worse)' if improvement < 0 else '')}")
        else:
            report.append("  Traditional average is zero, cannot calculate percentage improvement.")

    # Detailed Per-Test-Case Analysis
    report.append("\n\nDetailed Performance Per Test Case:")
    report.append("-" * 40)
    
    for test_case_name in df['test_case'].unique():
        report.append(f"\nTest Case: {test_case_name}")
        case_df = df[df['test_case'] == test_case_name]
        
        ai_row = case_df[case_df['mode'] == 'ai'].iloc[0] if not case_df[case_df['mode'] == 'ai'].empty else None
        trad_row = case_df[case_df['mode'] == 'traditional'].iloc[0] if not case_df[case_df['mode'] == 'traditional'].empty else None

        report.append("  AI-assisted:")
        if ai_row is not None:
            report.append(f"    Messages Sent: {ai_row['messages_sent']}, Received: {ai_row['messages_received']}")
            report.append(f"    Throughput: {ai_row['throughput_msg_per_sec']:.3f} msg/sec")
            report.append(f"    Avg Latency: {ai_row['avg_latency_sec']:.3f} sec")
            report.append(f"    Routing Accuracy: {ai_row['routing_accuracy']:.2%}")
        else:
            report.append("    No data available.")

        report.append("  Traditional:")
        if trad_row is not None:
            report.append(f"    Messages Sent: {trad_row['messages_sent']}, Received: {trad_row['messages_received']}")
            report.append(f"    Throughput: {trad_row['throughput_msg_per_sec']:.3f} msg/sec")
            report.append(f"    Avg Latency: {trad_row['avg_latency_sec']:.3f} sec")
            report.append(f"    Routing Accuracy: {trad_row['routing_accuracy']:.2%}")
        else:
            report.append("    No data available.")
            
    report_text = "\n".join(report)
    save_results(report_text, 'performance_report.txt') # Save as text file
    return report_text

def generate_visualizations(all_results):
    """Generate comparison charts."""
    df = pd.DataFrame(all_results)
    
    plt.style.use('seaborn-v0_8-darkgrid') # Use a nice seaborn style
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('AI vs. Traditional Routing Performance Comparison', fontsize=18, fontweight='bold')
    
    modes = ['ai', 'traditional']
    colors = {'ai': '#4CAF50', 'traditional': '#2196F3'} # Green for AI, Blue for Traditional
    
    # 1. Throughput Comparison
    throughput_data = df.groupby(['test_case', 'mode'])['throughput_msg_per_sec'].mean().unstack()
    throughput_data.plot(kind='bar', ax=axes[0, 0], color=[colors[m] for m in modes])
    axes[0, 0].set_title('Throughput (messages/sec)', fontsize=14, fontweight='bold')
    axes[0, 0].set_ylabel('Messages per second')
    axes[0, 0].set_xlabel('Test Case')
    axes[0, 0].tick_params(axis='x', rotation=45)
    axes[0, 0].legend(title='Mode')
    
    # 2. Latency Comparison
    latency_data = df.groupby(['test_case', 'mode'])['avg_latency_sec'].mean().unstack()
    latency_data.plot(kind='bar', ax=axes[0, 1], color=[colors[m] for m in modes])
    axes[0, 1].set_title('Average Latency (seconds)', fontsize=14, fontweight='bold')
    axes[0, 1].set_ylabel('Seconds')
    axes[0, 1].set_xlabel('Test Case')
    axes[0, 1].tick_params(axis='x', rotation=45)
    axes[0, 1].legend(title='Mode')
    
    # 3. Routing Accuracy
    accuracy_data = df.groupby(['test_case', 'mode'])['routing_accuracy'].mean().unstack()
    accuracy_data.plot(kind='bar', ax=axes[1, 0], color=[colors[m] for m in modes])
    axes[1, 0].set_title('Routing Accuracy', fontsize=14, fontweight='bold')
    axes[1, 0].set_ylabel('Accuracy (%)')
    axes[1, 0].set_xlabel('Test Case')
    axes[1, 0].set_ylim(0, 1.1) # Ensure y-axis goes up to 100%
    axes[1, 0].tick_params(axis='x', rotation=45)
    axes[1, 0].legend(title='Mode')
    
    # 4. Message Received vs. Sent
    received_sent_data = df.groupby(['test_case', 'mode'])[['messages_sent', 'messages_received']].mean()
    received_sent_data.unstack().plot(kind='bar', ax=axes[1, 1], 
                                     color=[colors['ai'], colors['ai'], colors['traditional'], colors['traditional']],
                                     edgecolor='black', alpha=0.7)
    axes[1, 1].set_title('Messages Sent vs. Received', fontsize=14, fontweight='bold')
    axes[1, 1].set_ylabel('Count')
    axes[1, 1].set_xlabel('Test Case')
    axes[1, 1].tick_params(axis='x', rotation=45)
    axes[1, 1].legend(['AI Sent', 'AI Received', 'Traditional Sent', 'Traditional Received'], title='Mode')
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout to prevent title overlap
    plot_filepath = os.path.join(RESULTS_DIR, 'performance_comparison.png')
    plt.savefig(plot_filepath, dpi=300)
    plt.close()
    print(f"Generated visualizations to {plot_filepath}")

def run_tests():
    """Main function to run the performance tests."""
    all_results = []
    
    print("="*60)
    print("Starting AI vs. Traditional Routing Performance Tests")
    print("="*60)
    print(f"Each test case will run for {TEST_DURATION} seconds with a {PUBLISH_INTERVAL}-second publish interval.")
    print(f"Warm-up time: {WARMUP_TIME}s, Cooldown time: {COOLDOWN_TIME}s.\n")
    
    for mode in TEST_MODES:
        print(f"\n--- Running tests for {mode.upper()} mode ---")
        system = TestSystem(mode)
        
        try:
            system.start() # Start broker and MQTT clients
            
            for test_case in TEST_CASES:
                results = system.run_test_case(test_case)
                all_results.append(results)
                time.sleep(1) # Brief pause between test cases
            
        finally:
            system.stop() # Ensure broker and clients are stopped
            print(f"--- Finished tests for {mode.upper()} mode ---\n")
            
    # --- Save and Analyze Results ---
    print("\n--- Generating Reports and Visualizations ---")
    save_results(all_results, 'raw_test_results.json')
    
    report_text = generate_comparison_report(all_results)
    print("\n--- Performance Report Summary ---")
    print(report_text) # Print the summary to console

    generate_visualizations(all_results)
    
    print("\n✅ All testing complete! Check the 'test_results/' directory for full reports.")
    print(f"  - Raw results: {os.path.join(RESULTS_DIR, 'raw_test_results.json')}")
    print(f"  - Textual report: {os.path.join(RESULTS_DIR, 'performance_report.txt')}")
    print(f"  - Visualizations: {os.path.join(RESULTS_DIR, 'performance_comparison.png')}")

if __name__ == "__main__":
    run_tests()
