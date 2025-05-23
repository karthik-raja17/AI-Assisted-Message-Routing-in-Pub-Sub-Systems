import paho.mqtt.client as mqtt
import time
from collections import deque
import json

class PerformanceMonitor:
    def __init__(self):
        self.metrics = {
            "message_count": 0,
            "critical_routed": 0,
            "normal_routed": 0,
            "latencies": deque(maxlen=100),
            "subscriber_counts": {"normal": [], "critical": []}
        }
        self.start_time = time.time()
        
        # Setup MQTT client to monitor broker activity
        self.client = mqtt.Client("PerformanceMonitor")
        self.client.on_message = self.on_message
        self.client.connect("localhost", 1883)
        self.client.subscribe("energy/#")
        
    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            self.metrics["message_count"] += 1
            
            # Track routing
            if "critical" in msg.topic:
                self.metrics["critical_routed"] += 1
            else:
                self.metrics["normal_routed"] += 1
                
            # Track latency if timestamp exists
            if "received_at" in data:
                processing_time = time.time() - float(data.get("timestamp", time.time()))
                self.metrics["latencies"].append(processing_time)
                
        except Exception as e:
            print(f"Monitoring error: {str(e)}")
            
    def record_subscriber_counts(self, normal_count, critical_count):
        self.metrics["subscriber_counts"]["normal"].append(normal_count)
        self.metrics["subscriber_counts"]["critical"].append(critical_count)
        
    def get_metrics(self):
        """Calculate and return performance metrics"""
        elapsed = time.time() - self.start_time
        throughput = self.metrics["message_count"] / elapsed if elapsed > 0 else 0
        
        latencies = list(self.metrics["latencies"])
        avg_latency = sum(latencies)/len(latencies) if latencies else 0
        
        return {
            "total_messages": self.metrics["message_count"],
            "critical_messages": self.metrics["critical_routed"],
            "normal_messages": self.metrics["normal_routed"],
            "throughput_msg_per_sec": throughput,
            "average_latency_sec": avg_latency,
            "subscriber_counts": self.metrics["subscriber_counts"]
        }

    def start(self):
        self.client.loop_start()
        
    def stop(self):
        self.client.loop_stop()
        return self.get_metrics()

if __name__ == "__main__":
    monitor = PerformanceMonitor()
    monitor.start()
    
    try:
        # Run monitoring for 5 minutes
        time.sleep(300)
    except KeyboardInterrupt:
        pass
        
    metrics = monitor.stop()
    print("\nPerformance Metrics:")
    print(json.dumps(metrics, indent=2))
