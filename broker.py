import paho.mqtt.client as mqtt
from openai import OpenAI
import json
from colorama import Fore, Style
import time
from collections import deque
from threading import Timer
from rule_manager import RuleManager
import statistics
import random
import subprocess
import psutil
import signal
import sys

class AIEnergyBroker:
    def __init__(self, groq_key: str):
        self.llm_client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_key
        )
        
        # Batch processing configuration
        self.normal_batch = deque()
        self.batch_size = 5
        self.batch_interval = 10
        self.batch_timer = None
        
        # Initialize Rule Manager
        self.rule_manager = RuleManager()
        
        # Scaling configuration
        self.scaling_enabled = True
        self.max_normal_subscribers = 4
        self.max_critical_subscribers = 3
        self.min_normal_subscribers = 1
        self.min_critical_subscribers = 1
        self.scaling_interval = 30  # seconds
        self.scaling_timer = None
        
        # Current active subscribers
        self.active_normal_subs = {
            "normal/subscriber1": {"weight": 2, "current_load": 0, "process": None},
            "normal/subscriber2": {"weight": 1, "current_load": 0, "process": None}
        }
        self.active_critical_subs = {
            "critical/subscriber1": {"weight": 2, "current_load": 0, "process": None},
            "critical/subscriber2": {"weight": 1, "current_load": 0, "process": None}
        }
        
        # MQTT Client setup
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            "AIEnergyBroker"
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # Connection retry logic
        connected = False
        retries = 3
        while not connected and retries > 0:
            try:
                self.client.connect("localhost", 1883, keepalive=60)
                connected = True
            except Exception as e:
                print(f"{Fore.RED}Connection failed, retrying... ({retries} attempts left){Style.RESET_ALL}")
                retries -= 1
                time.sleep(2)
        
        if not connected:
            raise RuntimeError("Failed to connect to MQTT broker after multiple attempts")

        # Start scaling monitor
        if self.scaling_enabled:
            self._start_scaling_engine()

    def _start_scaling_engine(self):
        """Start the periodic scaling check"""
        self.scaling_timer = Timer(self.scaling_interval, self._check_scaling_needs)
        self.scaling_timer.start()
        print(f"{Fore.CYAN}⚙️ Started auto-scaling engine (checks every {self.scaling_interval}s){Style.RESET_ALL}")

    def _check_scaling_needs(self):
        """Evaluate if scaling is needed"""
        try:
            if not self.scaling_enabled:
                return

            # Calculate normal subscribers load
            normal_loads = [sub['current_load']/sub['weight'] for sub in self.active_normal_subs.values()]
            avg_normal_load = sum(normal_loads)/len(normal_loads) if normal_loads else 0
            
            # Calculate critical subscribers load
            critical_loads = [sub['current_load']/sub['weight'] for sub in self.active_critical_subs.values()]
            avg_critical_load = sum(critical_loads)/len(critical_loads) if critical_loads else 0
            
            print(f"{Fore.CYAN}⚖️ Current loads - Normal: {avg_normal_load:.1%}, Critical: {avg_critical_load:.1%}{Style.RESET_ALL}")
            
            # Check normal subscribers scaling
            if avg_normal_load > 0.75 and len(self.active_normal_subs) < self.max_normal_subscribers:
                self._scale_up_normal()
            elif avg_normal_load < 0.25 and len(self.active_normal_subs) > self.min_normal_subscribers:
                self._scale_down_normal()
                
            # Check critical subscribers scaling
            if avg_critical_load > 0.75 and len(self.active_critical_subs) < self.max_critical_subscribers:
                self._scale_up_critical()
            elif avg_critical_load < 0.25 and len(self.active_critical_subs) > self.min_critical_subscribers:
                self._scale_down_critical()
                
        except Exception as e:
            print(f"{Fore.RED}Scaling check failed: {str(e)}{Style.RESET_ALL}")
        finally:
            if self.scaling_enabled:
                self._start_scaling_engine()

    def _scale_up_normal(self):
        """Add a new normal subscriber"""
        new_id = str(len(self.active_normal_subs) + 1)  # Just the number as string
        topic_name = f"normal/subscriber{new_id}"  # Full topic path
        
        if topic_name not in self.active_normal_subs:
            try:
                proc = subprocess.Popen(
                    ["python", "normal_subscriber.py", new_id],  # Pass just the ID number
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                self.active_normal_subs[topic_name] = {
                    "weight": 1,
                    "current_load": 0,
                    "process": proc
                }
                
                print(f"{Fore.GREEN}🔼 Scaled UP normal subscribers: Added {topic_name}{Style.RESET_ALL}")
                
                for sub in self.active_normal_subs.values():
                    sub['weight'] = max(1, sub['weight'] - 0.1)
                
            except Exception as e:
                print(f"{Fore.RED}Failed to scale up normal subscribers: {str(e)}{Style.RESET_ALL}")

    def _scale_down_normal(self):
        """Remove a normal subscriber"""
        if len(self.active_normal_subs) > self.min_normal_subscribers:
            try:
                to_remove = min(
                    self.active_normal_subs.items(),
                    key=lambda x: x[1]['current_load']/x[1]['weight']
                )
                
                if to_remove[1]['process']:
                    to_remove[1]['process'].terminate()
                
                del self.active_normal_subs[to_remove[0]]
                
                print(f"{Fore.YELLOW}🔽 Scaled DOWN normal subscribers: Removed {to_remove[0]}{Style.RESET_ALL}")
                
                for sub in self.active_normal_subs.values():
                    sub['weight'] = min(3, sub['weight'] + 0.2)
                
            except Exception as e:
                print(f"{Fore.RED}Failed to scale down normal subscribers: {str(e)}{Style.RESET_ALL}")

    def _scale_up_critical(self):
        """Add a new critical subscriber"""
        new_id = str(len(self.active_critical_subs) + 1)  # Just the number as string
        topic_name = f"critical/subscriber{new_id}"  # Full topic path
        
        if topic_name not in self.active_critical_subs:
            try:
                proc = subprocess.Popen(
                    ["python", "red_subscriber.py", new_id],  # Pass just the ID number
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                self.active_critical_subs[topic_name] = {
                    "weight": 1,
                    "current_load": 0,
                    "process": proc
                }
                
                print(f"{Fore.GREEN}🔼 Scaled UP critical subscribers: Added {topic_name}{Style.RESET_ALL}")
                
                for sub in self.active_critical_subs.values():
                    sub['weight'] = max(1, sub['weight'] - 0.1)
                
            except Exception as e:
                print(f"{Fore.RED}Failed to scale up critical subscribers: {str(e)}{Style.RESET_ALL}")

    def _scale_down_critical(self):
        """Remove a critical subscriber"""
        if len(self.active_critical_subs) > self.min_critical_subscribers:
            try:
                to_remove = min(
                    self.active_critical_subs.items(),
                    key=lambda x: x[1]['current_load']/x[1]['weight']
                )
                
                if to_remove[1]['process']:
                    to_remove[1]['process'].terminate()
                
                del self.active_critical_subs[to_remove[0]]
                
                print(f"{Fore.YELLOW}🔽 Scaled DOWN critical subscribers: Removed {to_remove[0]}{Style.RESET_ALL}")
                
                for sub in self.active_critical_subs.values():
                    sub['weight'] = min(3, sub['weight'] + 0.2)
                
            except Exception as e:
                print(f"{Fore.RED}Failed to scale down critical subscribers: {str(e)}{Style.RESET_ALL}")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"{Fore.GREEN}Broker connected to MQTT server (RC: {rc}){Style.RESET_ALL}")
            client.subscribe("building/energy", qos=1)
            print(f"{Fore.CYAN}Subscribed to 'building/energy' topic{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Broker connection failed (RC: {rc}){Style.RESET_ALL}")

    def select_subscriber(self, subscriber_pool):
        total_weight = sum(sub['weight'] for sub in subscriber_pool.values())
        selection = random.uniform(0, total_weight)
        
        current = 0
        for sub_name, sub_info in subscriber_pool.items():
            adjusted_weight = max(1, sub_info['weight'] - sub_info['current_load'])
            current += adjusted_weight
            if selection <= current:
                return sub_name
        
        return next(iter(subscriber_pool.keys()))  # fallback

    def update_load(self, sub_name, change):
        """Update the load for a specific subscriber"""
        if "normal" in sub_name:
            pool = self.active_normal_subs
        else:
            pool = self.active_critical_subs
            
        if sub_name in pool:
            pool[sub_name]['current_load'] += change
            pool[sub_name]['current_load'] = max(0, pool[sub_name]['current_load'])

    def ai_analyze(self, data) -> tuple:
        current_power_threshold = self.rule_manager.rules['high_power']['conditions'][0]['value']
        current_temp_threshold = self.rule_manager.rules['high_temp']['conditions'][0]['value']
        
        prompt = f"""
        Analyze this industrial energy system data (STRICTLY FOLLOW FORMAT):

        CURRENT THRESHOLDS:
        - Power Alert: > {current_power_threshold:.1f}W or < 100W
        - Temp Alert: > {current_temp_threshold:.1f}°C

        ENERGY DATA:
        - Total: {data['energy']['total']}W
        - Lights: {data['energy']['lights']}W

        ENVIRONMENT:
        - Hottest Zone: {max(z['temperature'] for z in data['zones'].values()):.1f}°C
        - Avg Humidity: {sum(z['humidity'] for z in data['zones'].values())/9:.1f}%

        DECISION RULES:
        1. CRITICAL if:
           - Power > {current_power_threshold:.1f}W OR < 100W
           - Any zone > {current_temp_threshold:.1f}°C
           - Abnormal patterns detected
        2. Otherwise NORMAL

        RESPONSE FORMAT:
        priority|analysis_summary
        """
        
        try:
            response = self.llm_client.chat.completions.create(
                model="llama3-70b-8192",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            return self._parse_ai_response(response.choices[0].message.content)
        except Exception as e:
            print(f"{Fore.RED}AI analysis failed: {str(e)}{Style.RESET_ALL}")
            return ("normal", "AI analysis failed")

    def _parse_ai_response(self, response: str) -> tuple:
        try:
            parts = response.split("|", 1)
            if len(parts) == 2:
                priority, analysis = parts
                priority = priority.strip().lower()
                if priority not in ['normal', 'critical']:
                    print(f"{Fore.YELLOW}Invalid priority '{priority}', defaulting to normal{Style.RESET_ALL}")
                    priority = 'normal'
                return (priority, analysis.strip())
            return ("normal", response.strip())
        except Exception as e:
            print(f"{Fore.YELLOW}Failed to parse AI response: {str(e)}{Style.RESET_ALL}")
            return ("normal", "AI analysis parsing failed")

    def process_normal_batch(self):
        if not self.normal_batch:
            return
            
        print(f"{Fore.CYAN}Processing batch of {len(self.normal_batch)} normal messages{Style.RESET_ALL}")
        
        selected_sub = self.select_subscriber(self.active_normal_subs)
        subscriber_num = selected_sub.split('subscriber')[-1]  # Extract just the number
        self.update_load(selected_sub, +1)
        
        batch_data = {
            "messages": list(self.normal_batch),
            "batch_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "batch_size": len(self.normal_batch),
            "ai_priority": "normal",
            "ai_analysis": "Batch processed normal messages",
            "target_subscriber": subscriber_num
        }
        
        self.client.publish(
            f"energy/normal/subscriber{subscriber_num}",  # Use just the number
            payload=json.dumps(batch_data),
            qos=1
        )
        
        Timer(2, self.update_load, args=(selected_sub, -1)).start()
        self.normal_batch.clear()
        self._reset_batch_timer()

    def _reset_batch_timer(self):
        if self.batch_timer:
            self.batch_timer.cancel()
        self.batch_timer = Timer(self.batch_interval, self.process_normal_batch)
        self.batch_timer.start()

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            current_power = data['energy']['total']
            
            self.rule_manager.update_stats(data)
            threshold_changed = self.rule_manager.adapt_rules()
            
            ai_priority, ai_analysis = self.ai_analyze(data)
            rule_priority, action = self.rule_manager.evaluate_message(data)
            
            final_priority = "critical" if ("critical" in [ai_priority, rule_priority]) else "normal"
            
            if final_priority == "critical":
                selected_sub = self.select_subscriber(self.active_critical_subs)
                subscriber_num = selected_sub.split('subscriber')[-1]  # Extract just the number
                self.update_load(selected_sub, +1)
                
                print(f"{Fore.RED}CRITICAL ALERT! Routing to {selected_sub}{Style.RESET_ALL}")
                client.publish(
                    f"energy/critical/subscriber{subscriber_num}",
                    payload=json.dumps({
                        **data,
                        "ai_priority": ai_priority,
                        "ai_analysis": ai_analysis,
                        "rule_priority": rule_priority,
                        "rule_trigger": action,
                        "current_threshold": self.rule_manager.rules['high_power']['conditions'][0]['value'],
                        "received_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "target_subscriber": subscriber_num
                    }),
                    qos=1
                )
                
                Timer(5, self.update_load, args=(selected_sub, -1)).start()
            else:
                self.normal_batch.append({
                    **data,
                    "ai_priority": final_priority,
                    "ai_analysis": f"AI:{ai_priority}, Rule:{rule_priority}",
                    "received_at": time.strftime("%Y-%m-%d %H:%M:%S")
                })
                
                print(f"{Fore.GREEN}Added to normal batch (size: {len(self.normal_batch)}){Style.RESET_ALL}")
                
                if not self.batch_timer:
                    self._reset_batch_timer()
                if len(self.normal_batch) >= self.batch_size:
                    self.process_normal_batch()
            
        except Exception as e:
            print(f"{Fore.RED}Error processing message: {str(e)}{Style.RESET_ALL}")

    def start(self):
        try:
            print(f"{Fore.BLUE}🚀 Starting AI-Enabled MQTT Broker with Auto-Scaling{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Initial Thresholds:{Style.RESET_ALL}")
            print(f"- High Power: {self.rule_manager.rules['high_power']['conditions'][0]['value']}W")
            print(f"- High Temp: {self.rule_manager.rules['high_temp']['conditions'][0]['value']}°C")
            print(f"{Fore.CYAN}Auto-Scaling Configuration:{Style.RESET_ALL}")
            print(f"- Normal Subscribers: {len(self.active_normal_subs)}/{self.max_normal_subscribers}")
            print(f"- Critical Subscribers: {len(self.active_critical_subs)}/{self.max_critical_subscribers}")
            self.client.loop_forever()
        except KeyboardInterrupt:
            print(f"{Fore.YELLOW}\n🛑 Stopping broker...{Style.RESET_ALL}")
            if self.batch_timer:
                self.batch_timer.cancel()
            if self.scaling_timer:
                self.scaling_timer.cancel()
            if self.normal_batch:
                self.process_normal_batch()
            # Clean up subscriber processes
            for sub in list(self.active_normal_subs.values()) + list(self.active_critical_subs.values()):
                if sub['process']:
                    sub['process'].terminate()
            self.client.disconnect()
        except Exception as e:
            print(f"{Fore.RED}⛔ Broker error: {str(e)}{Style.RESET_ALL}")

if __name__ == "__main__":
    broker = AIEnergyBroker("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")
    broker.start()
