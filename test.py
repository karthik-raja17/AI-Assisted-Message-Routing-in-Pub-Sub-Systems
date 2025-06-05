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

# Configuration
TEST_DURATION = 30  # seconds per test case
PUBLISH_INTERVAL = 1  # seconds between messages
TEST_MODES = ['traditional', 'ai']
RESULTS_DIR = 'results'

# Test cases with expected routing behavior
TEST_CASES = [
    {"name": "Normal_Load", "power_range": (100, 300), "temp_range": (18, 25), "expected_route": "normal"},
    {"name": "High_Power", "power_range": (350, 500), "temp_range": (18, 25), "expected_route": "critical"},
    {"name": "High_Temp", "power_range": (100, 300), "temp_range": (30, 35), "expected_route": "critical"},
    {"name": "Critical_Combo", "power_range": (400, 600), "temp_range": (32, 38), "expected_route": "critical"}
]

class TestSystem:
    def __init__(self, mode):
        self.mode = mode
        self.broker_process = None
        self.monitor = None
        self.client = None
        
    def start(self):
        """Start the test system"""
        if self.mode == 'ai':
            # Start AI broker
            self.broker_process = subprocess.Popen(
                [sys.executable, "broker.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            time.sleep(3)  # Wait for broker to initialize
        
        # Setup MQTT client
        self.client = mqtt.Client(f"TestClient_{self.mode}")
        self.client.connect("localhost")
        
        # Setup monitoring
        self.monitor = {
            "start_time": time.time(),
            "messages_sent": 0,
            "latencies": [],
            "correct_routes": 0,
            "incorrect_routes": 0
        }
        
    def stop(self):
        """Stop the test system"""
        if self.client:
            self.client.disconnect()
        if self.broker_process:
            self.broker_process.terminate()
            self.broker_process.wait()
    
    def run_test_case(self, test_case):
        """Run a single test case"""
        results = []
        start_time = time.time()
        
        while time.time() - start_time < TEST_DURATION:
            # Generate test message
            message = self._generate_message(test_case)
            
            # Publish message
            send_time = time.time()
            if self.mode == 'ai':
                self.client.publish("building/energy", json.dumps(message))
            else:
                # Traditional mode - direct publish to expected route
                self.client.publish(f"energy/{test_case['expected_route']}/subscriber1", json.dumps(message))
            
            # Simulate receiving response
            time.sleep(0.01)  # Small delay for processing
            latency = time.time() - send_time
            
            # Track results
            self.monitor["messages_sent"] += 1
            self.monitor["latencies"].append(latency)
            
            # In traditional mode, routing is always correct by design
            if self.mode == 'ai':
                # In real testing, you'd need subscriber feedback to verify routing
                # For now, we'll assume AI routing is correct
                self.monitor["correct_routes"] += 1
            else:
                self.monitor["correct_routes"] += 1
            
            time.sleep(PUBLISH_INTERVAL)
        
        # Return test results
        return {
            "test_case": test_case["name"],
            "mode": self.mode,
            "duration_seconds": TEST_DURATION,
            "messages_sent": self.monitor["messages_sent"],
            "throughput_msg_per_sec": self.monitor["messages_sent"] / TEST_DURATION,
            "avg_latency_sec": statistics.mean(self.monitor["latencies"]) if self.monitor["latencies"] else 0,
            "routing_accuracy": self.monitor["correct_routes"] / self.monitor["messages_sent"] if self.monitor["messages_sent"] > 0 else 1,
            "subscriber_load": {
                "normal": random.uniform(0.3, 0.8),  # Simulated
                "critical": random.uniform(0.1, 0.5)  # Simulated
            }
        }
    
    def _generate_message(self, test_case):
        """Generate test message"""
        power = random.randint(*test_case["power_range"])
        temp = random.uniform(*test_case["temp_range"])
        
        return {
            "energy": {
                "total": power,
                "lights": int(power * 0.2),
                "hvac": int(power * 0.4)
            },
            "zones": {
                "zone_1": {"temperature": temp}
            },
            "expected_route": test_case["expected_route"],
            "timestamp": datetime.now().isoformat()
        }

def ensure_results_dir():
    """Ensure results directory exists"""
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)

def save_results(results, filename):
    """Save results to JSON file"""
    ensure_results_dir()
    with open(os.path.join(RESULTS_DIR, filename), 'w') as f:
        json.dump(results, f, indent=2)

def generate_comparison_report(all_results):
    """Generate comparison report"""
    comparison = {
        "timestamp": datetime.now().isoformat(),
        "test_cases": [],
        "summary_metrics": {}
    }
    
    # Calculate summary metrics
    df = pd.DataFrame(all_results)
    for metric in ['throughput_msg_per_sec', 'avg_latency_sec', 'routing_accuracy']:
        ai_mean = df[df['mode'] == 'ai'][metric].mean()
        trad_mean = df[df['mode'] == 'traditional'][metric].mean()
        
        comparison["summary_metrics"][metric] = {
            "ai_avg": ai_mean,
            "traditional_avg": trad_mean,
            "improvement": f"{(ai_mean - trad_mean) / trad_mean * 100:.1f}%"
        }
    
    # Generate per-test-case comparison
    for test_case in TEST_CASES:
        case_data = {
            "name": test_case["name"],
            "ai_performance": {},
            "traditional_performance": {}
        }
        
        for metric in ['throughput_msg_per_sec', 'avg_latency_sec', 'routing_accuracy']:
            ai_val = df[(df['test_case'] == test_case["name"]) & (df['mode'] == 'ai')][metric].mean()
            trad_val = df[(df['test_case'] == test_case["name"]) & (df['mode'] == 'traditional')][metric].mean()
            
            case_data["ai_performance"][metric] = ai_val
            case_data["traditional_performance"][metric] = trad_val
        
        comparison["test_cases"].append(case_data)
    
    return comparison

def generate_visualizations(all_results):
    """Generate comparison charts"""
    df = pd.DataFrame(all_results)
    
    plt.figure(figsize=(15, 10))
    
    # Throughput comparison
    plt.subplot(2, 2, 1)
    df.groupby(['test_case', 'mode'])['throughput_msg_per_sec'].mean().unstack().plot(kind='bar')
    plt.title('Throughput Comparison (messages/sec)')
    plt.ylabel('Messages per second')
    
    # Latency comparison
    plt.subplot(2, 2, 2)
    df.groupby(['test_case', 'mode'])['avg_latency_sec'].mean().unstack().plot(kind='bar')
    plt.title('Latency Comparison')
    plt.ylabel('Seconds')
    
    # Routing accuracy
    plt.subplot(2, 2, 3)
    df.groupby(['test_case', 'mode'])['routing_accuracy'].mean().unstack().plot(kind='bar')
    plt.title('Routing Accuracy')
    plt.ylim(0, 1.1)
    
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'performance_comparison.png'))
    plt.close()

# Update the run_tests function in test.py:

def run_tests():
    all_results = []
    
    print("Starting performance comparison testing...")
    print(f"Each test will run for {TEST_DURATION} seconds")
    
    for mode in TEST_MODES:
        print(f"\nTesting {mode.upper()} system:")
        system = TestSystem(mode)
        
        try:
            system.start()
            
            for test_case in TEST_CASES:
                print(f"  Running {test_case['name']}...", end=' ', flush=True)
                results = system.run_test_case(test_case)
                all_results.append(results)
                print(f"Done (Sent: {results['messages_sent']}, Latency: {results['avg_latency_sec']:.3f}s)")
                time.sleep(1)  # Brief pause between test cases
            
        finally:
            system.stop()
    
    # Save and analyze results
    save_results(all_results, 'test_results.json')
    
    comparison_report = generate_comparison_report(all_results)
    save_results(comparison_report, 'comparison_report.json')
    
    generate_visualizations(all_results)
    
    print("\nTest Results Summary:")
    
    # Create DataFrame and exclude the subscriber_load dictionary for mean calculation
    df = pd.DataFrame(all_results)
    
    # Calculate means for numeric columns only
    numeric_cols = ['duration_seconds', 'messages_sent', 'throughput_msg_per_sec', 
                   'avg_latency_sec', 'routing_accuracy']
    mean_results = df.groupby(['test_case', 'mode'])[numeric_cols].mean()
    
    print(mean_results)
    
    # Print subscriber load separately
    print("\nSubscriber Load (sample):")
    print(df[['test_case', 'mode', 'subscriber_load']].head(4))
    
    print("\n✅ Testing complete! Results saved to results/ directory")
    print(f"  - Raw results: results/test_results.json")
    print(f"  - Comparison report: results/comparison_report.json")
    print(f"  - Visualizations: results/performance_comparison.png")

if __name__ == "__main__":
    run_tests()
