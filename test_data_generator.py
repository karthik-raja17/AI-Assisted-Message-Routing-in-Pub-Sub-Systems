import json
import random
import time
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt

class TestDataGenerator:
    def __init__(self):
        self.client = mqtt.Client("TestDataGenerator")
        self.client.connect("localhost", 1883)
        self.base_time = datetime.now()
        self.message_counter = 0
        
    def _publish_with_stats(self, message, is_critical=False):
        """Publish message and record statistics"""
        self.message_counter += 1
        message['_test_seq'] = self.message_counter
        message['_test_critical'] = is_critical
        
        self.client.publish("building/energy", json.dumps(message))
        time.sleep(0.1)  # Throttle messages
        
    def generate_normal_data(self, count=50):
        """Generate normal operating condition data"""
        for i in range(count):
            timestamp = (self.base_time + timedelta(minutes=i)).isoformat()
            message = {
                "timestamp": timestamp,
                "energy": {
                    "total": random.randint(150, 250),
                    "lights": random.randint(20, 50),
                    "hvac": random.randint(50, 100),
                    "equipment": random.randint(50, 100)
                },
                "zones": {
                    f"zone_{j}": {
                        "temperature": round(random.uniform(18, 24)), 
                        "humidity": random.randint(40, 60)
                    } for j in range(1, 5)
                }
            }
            self._publish_with_stats(message)
            
    def generate_critical_power(self, count=10):
        """Generate power surge scenarios"""
        for i in range(count):
            timestamp = (self.base_time + timedelta(minutes=i)).isoformat()
            message = {
                "timestamp": timestamp,
                "energy": {
                    "total": random.randint(350, 500),
                    "lights": random.randint(30, 60),
                    "hvac": random.randint(150, 250),
                    "equipment": random.randint(150, 250)
                },
                "zones": self._generate_normal_zones()
            }
            self._publish_with_stats(message, is_critical=True)
            time.sleep(0.5)
            
    def generate_critical_temp(self, count=5):
        """Generate temperature alert scenarios"""
        for i in range(count):
            timestamp = (self.base_time + timedelta(minutes=i)).isoformat()
            zones = {
                f"zone_{j}": {
                    "temperature": round(random.uniform(18, 22)), 
                    "humidity": random.randint(40, 60)
                } for j in range(1, 4)
            }
            # Make one zone critical
            zones["zone_4"] = {
                "temperature": round(random.uniform(32, 38)), 
                "humidity": random.randint(30, 50)
            }
            
            message = {
                "timestamp": timestamp,
                "energy": {
                    "total": random.randint(150, 250),
                    "lights": random.randint(20, 50),
                    "hvac": random.randint(80, 120),
                    "equipment": random.randint(50, 100)
                },
                "zones": zones
            }
            self._publish_with_stats(message, is_critical=True)
            time.sleep(0.5)
            
    def generate_ai_test_cases(self, count=3):
        """Generate specific test cases for AI analysis evaluation"""
        test_messages = [
            # Borderline case (should trigger AI analysis)
            {
                "energy": {"total": 330, "lights": 40, "hvac": 120, "equipment": 170},
                "zones": {"zone1": {"temperature": 28}, "zone2": {"temperature": 26}}
            },
            # Clearly critical case
            {
                "energy": {"total": 450, "lights": 50, "hvac": 200, "equipment": 200},
                "zones": {"zone1": {"temperature": 35}, "zone2": {"temperature": 30}}
            }
        ]
        
        for msg in test_messages[:count]:
            msg["timestamp"] = (self.base_time + timedelta(minutes=self.message_counter)).isoformat()
            self._publish_with_stats(msg, is_critical=True)
            time.sleep(5)  # Space out API calls

    def _generate_normal_zones(self):
        return {
            f"zone_{j}": {
                "temperature": round(random.uniform(18, 22)), 
                "humidity": random.randint(40, 60)
            } for j in range(1, 5)
        }
