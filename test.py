import time
import random
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import os

# Test Configuration
TEST_DURATION = 30  # seconds (reduced for quicker testing)
PUBLISH_INTERVAL = 1  # seconds
TEST_CASES = [
    {"name": "Normal_Load", "power_range": (100, 300), "temp_range": (18, 25)},
    {"name": "High_Power", "power_range": (350, 500), "temp_range": (18, 25)},
    {"name": "High_Temp", "power_range": (100, 300), "temp_range": (30, 35)},
    {"name": "Critical_Combo", "power_range": (400, 600), "temp_range": (32, 38)}
]

# Add this to your test script before running tests
def check_connections():
    try:
        test_client = mqtt.Client("ConnectionTester")
        test_client.connect("localhost", 1883, 10)
        test_client.subscribe("energy/#")
        test_client.loop_start()
        time.sleep(1)  # Wait for connection
        test_client.disconnect()
        return True
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False

if not check_connections():
    print("Cannot connect to broker. Make sure it's running!")
    exit(1)

def generate_test_message(test_case):
    """Generate synthetic test data"""
    power = random.randint(*test_case["power_range"])
    temp = random.uniform(*test_case["temp_range"])
    
    return {
        "energy": {
            "total": power,
            "lights": int(power * 0.2),
            "hvac": int(power * 0.4),
            "equipment": int(power * 0.4)
        },
        "zones": {
            "zone_1": {"temperature": temp, "humidity": random.randint(30, 70)},
            "zone_2": {"temperature": temp - random.uniform(1, 3), "humidity": random.randint(30, 70)}
        },
        "received_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def run_test_case(test_case):
    """Execute a single test case"""
    results = {
        "test_name": test_case["name"],
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "messages_sent": 0,
        "messages_routed": {"normal": 0, "critical": 0},
        "latencies": []
    }
    
    def on_message(client, userdata, msg):
        try:
            topic = msg.topic.split('/')[1]
            if topic in ['normal', 'critical']:
                results["messages_routed"][topic] += 1
                
                data = json.loads(msg.payload.decode())
                if 'received_at' in data:
                    sent_time = datetime.strptime(data['received_at'], "%Y-%m-%d %H:%M:%S").timestamp()
                    latency = time.time() - sent_time
                    results["latencies"].append(latency)
        except Exception as e:
            print(f"Error processing message: {e}")

    # Setup clients
    test_client = mqtt.Client("TestMonitor")
    test_client.on_message = on_message
    test_client.connect("localhost", 1883)
    test_client.subscribe("energy/#")
    test_client.loop_start()
    
    pub_client = mqtt.Client("TestPublisher")
    pub_client.connect("localhost", 1883)
    
    # Run test
    start_time = time.time()
    while time.time() - start_time < TEST_DURATION:
        message = generate_test_message(test_case)
        pub_client.publish("building/energy", json.dumps(message))
        results["messages_sent"] += 1
        time.sleep(PUBLISH_INTERVAL)
    
    # Cleanup
    test_client.loop_stop()
    test_client.disconnect()
    pub_client.disconnect()
    
    # Calculate metrics
    results["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results["duration_seconds"] = TEST_DURATION
    if results["latencies"]:
        results["avg_latency"] = sum(results["latencies"]) / len(results["latencies"])
    else:
        results["avg_latency"] = 0
    
    # Save to file
    filename = f"test_results_{test_case['name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"Saved results to {filename}")
    return results

def main():
    print("Starting AI Routing System Evaluation")
    print(f"Each test will run for {TEST_DURATION} seconds")
    
    all_results = []
    for test_case in TEST_CASES:
        results = run_test_case(test_case)
        all_results.append(results)
        time.sleep(2)  # Brief pause between tests
    
    # Save consolidated results
    summary_filename = "test_summary.json"
    with open(summary_filename, 'w') as f:
        json.dump({
            "test_start": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "test_cases": all_results
        }, f, indent=2)
    
    print(f"\nSaved consolidated results to {summary_filename}")
    print("Testing complete!")

if __name__ == "__main__":
    main()
