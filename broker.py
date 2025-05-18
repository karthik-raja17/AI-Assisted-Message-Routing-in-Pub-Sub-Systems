#!/usr/bin/env python3
"""
AI-Enabled MQTT Energy Broker with Auto-Scaling Subscribers
"""

import paho.mqtt.client as mqtt
from openai import OpenAI
import json
from colorama import Fore, Style
import time
from collections import deque
from threading import Timer
from rule_manager import RuleManager
import random
import subprocess
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format=f"{Fore.CYAN}%(asctime)s{Style.RESET_ALL} - "
           f"{Fore.GREEN}%(levelname)s{Style.RESET_ALL} - "
           f"%(message)s",
    handlers=[
        logging.FileHandler('broker.log'),
        logging.StreamHandler()
    ]
)

class AIEnergyBroker:
    def __init__(self, groq_key: str):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"{Fore.YELLOW}Initializing AIEnergyBroker...{Style.RESET_ALL}")
        
        # Initialize AI Client with rate limit handling
        self.llm_client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_key,
            max_retries=2  # Add retry configuration
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
        self.active_normal_subs = {}
        self.active_critical_subs = {}
        self._initialize_subscribers()
        
        # MQTT Client setup
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            "AIEnergyBroker"
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        
        # Connect to MQTT broker
        self._connect_mqtt()

        # Start scaling monitor
        if self.scaling_enabled:
            self._start_scaling_engine()

    def _initialize_subscribers(self):
        """Initialize default subscriber processes"""
        try:
            # Start normal subscribers
            for i in range(1, 3):
                sub_name = f"normal/subscriber{i}"
                self._start_and_register_subscriber("normal", str(i), sub_name, self.active_normal_subs)
            
            # Start critical subscribers
            for i in range(1, 3):
                sub_name = f"critical/subscriber{i}"
                self._start_and_register_subscriber("critical", str(i), sub_name, self.active_critical_subs)
                
        except Exception as e:
            self.logger.error(f"{Fore.RED}Failed to initialize subscribers: {str(e)}{Style.RESET_ALL}")
            raise

    def _start_and_register_subscriber(self, sub_type: str, sub_id: str, sub_name: str, pool: dict):
        """Start a subscriber process and register it in the pool"""
        try:
            script = "normal_subscriber.py" if sub_type == "normal" else "red_subscriber.py"
            proc = subprocess.Popen(
                [sys.executable, script, sub_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            pool[sub_name] = {
                "weight": 2 if sub_id == "1" else 1,
                "current_load": 0,
                "process": proc
            }
            self.logger.info(f"Started {sub_name} (PID: {proc.pid})")
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Failed to start {sub_name}: {str(e)}{Style.RESET_ALL}")
            raise

    def _connect_mqtt(self):
        """Connect to MQTT broker with retry logic"""
        connected = False
        retries = 3
        while not connected and retries > 0:
            try:
                self.client.connect("localhost", 1883, keepalive=60)
                connected = True
                self.logger.info(f"{Fore.GREEN}Connected to MQTT broker{Style.RESET_ALL}")
            except Exception as e:
                self.logger.warning(f"{Fore.YELLOW}Connection failed (attempts left: {retries}): {str(e)}{Style.RESET_ALL}")
                retries -= 1
                time.sleep(2)
        
        if not connected:
            raise RuntimeError("Failed to connect to MQTT broker")

    def _start_scaling_engine(self):
        """Start the periodic scaling check"""
        self.scaling_timer = Timer(self.scaling_interval, self._check_scaling_needs)
        self.scaling_timer.start()
        self.logger.info(f"Started auto-scaling engine (checks every {self.scaling_interval}s)")

    def _check_scaling_needs(self):
        """Evaluate if scaling is needed"""
        try:
            if not self.scaling_enabled:
                return

            # Calculate loads
            avg_normal_load = self._calculate_avg_load(self.active_normal_subs)
            avg_critical_load = self._calculate_avg_load(self.active_critical_subs)
            
            self.logger.info(f"Current loads - Normal: {avg_normal_load:.1%}, Critical: {avg_critical_load:.1%}")
            
            # Check scaling needs
            self._check_normal_scaling(avg_normal_load)
            self._check_critical_scaling(avg_critical_load)
                
        except Exception as e:
            self.logger.error(f"{Fore.RED}Scaling check failed: {str(e)}{Style.RESET_ALL}")
        finally:
            if self.scaling_enabled:
                self._start_scaling_engine()

    def _calculate_avg_load(self, pool: dict) -> float:
        """Calculate average load for a subscriber pool"""
        loads = [sub['current_load']/sub['weight'] for sub in pool.values()]
        return sum(loads)/len(loads) if loads else 0

    def _check_normal_scaling(self, avg_load: float):
        """Check if normal subscribers need scaling"""
        if (avg_load > 0.75 and len(self.active_normal_subs) < self.max_normal_subscribers):
            self._scale_up("normal", self.active_normal_subs, self.max_normal_subscribers)
        elif (avg_load < 0.25 and len(self.active_normal_subs) > self.min_normal_subscribers):
            self._scale_down("normal", self.active_normal_subs, self.min_normal_subscribers)

    def _check_critical_scaling(self, avg_load: float):
        """Check if critical subscribers need scaling"""
        if (avg_load > 0.75 and len(self.active_critical_subs) < self.max_critical_subscribers):
            self._scale_up("critical", self.active_critical_subs, self.max_critical_subscribers)
        elif (avg_load < 0.25 and len(self.active_critical_subs) > self.min_critical_subscribers):
            self._scale_down("critical", self.active_critical_subs, self.min_critical_subscribers)

    def _scale_up(self, sub_type: str, pool: dict, max_subs: int):
        """Generic scale-up method"""
        if len(pool) >= max_subs:
            self.logger.warning(f"Max {sub_type} subscribers reached")
            return

        try:
            new_id = str(len(pool) + 1)
            sub_name = f"{sub_type}/subscriber{new_id}"
            
            script = "normal_subscriber.py" if sub_type == "normal" else "red_subscriber.py"
            proc = subprocess.Popen(
                [sys.executable, script, new_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            pool[sub_name] = {
                "weight": 1,
                "current_load": 0,
                "process": proc
            }
            
            # Adjust weights of existing subscribers
            for sub in pool.values():
                sub['weight'] = max(1, sub['weight'] - 0.1)
            
            self.logger.info(f"🔼 Scaled UP {sub_type} subscribers: Added {sub_name}")
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}{sub_type} scale-up failed: {str(e)}{Style.RESET_ALL}")

    def _scale_down(self, sub_type: str, pool: dict, min_subs: int):
        """Generic scale-down method"""
        if len(pool) > min_subs:
            try:
                to_remove = min(
                    pool.items(),
                    key=lambda x: x[1]['current_load']/x[1]['weight']
                )
                
                if to_remove[1]['process']:
                    to_remove[1]['process'].terminate()
                    to_remove[1]['process'].wait(timeout=5)
                
                del pool[to_remove[0]]
                
                self.logger.info(f"🔽 Scaled DOWN {sub_type} subscribers: Removed {to_remove[0]}")
                
                for sub in pool.values():
                    sub['weight'] = min(3, sub['weight'] + 0.2)
                
            except Exception as e:
                self.logger.error(f"{Fore.RED}Failed to scale down {sub_type} subscribers: {str(e)}{Style.RESET_ALL}")

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Handle MQTT connection events"""
        if rc == 0:
            self.logger.info(f"{Fore.GREEN}Connected to MQTT server (RC: {rc}){Style.RESET_ALL}")
            client.subscribe("building/energy", qos=1)
            self.logger.info("Subscribed to 'building/energy' topic")
        else:
            self.logger.error(f"{Fore.RED}Connection failed (RC: {rc}){Style.RESET_ALL}")

    def _on_message(self, client, userdata, msg):
        """Process incoming MQTT messages"""
        try:
            data = json.loads(msg.payload.decode())
            self.logger.debug(f"Received message on {msg.topic}")
            
            # Update rules and get priorities
            self.rule_manager.update_stats(data)
            threshold_changed = self.rule_manager.adapt_rules()
            
            # Get AI analysis with fallback
            try:
                ai_priority, ai_analysis = self.ai_analyze(data)
                self.logger.info(f"AI Analysis - Priority: {ai_priority}, Analysis: {ai_analysis}")
            except Exception as e:
                self.logger.warning(f"{Fore.YELLOW}AI analysis failed, using fallback: {str(e)}{Style.RESET_ALL}")
                ai_priority = "normal"
                ai_analysis = "AI analysis unavailable"
            
            rule_priority, action = self.rule_manager.evaluate_message(data)
            
            # Determine final priority
            final_priority = "critical" if ("critical" in [ai_priority, rule_priority]) else "normal"
            
            if final_priority == "critical":
                self._handle_critical_message(client, data, ai_priority, ai_analysis, rule_priority, action)
            else:
                self._handle_normal_message(data, ai_priority, rule_priority)
            
        except Exception as e:
            self.logger.error(f"{Fore.RED}Error processing message: {str(e)}{Style.RESET_ALL}")

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """Handle publish confirmation"""
        self.logger.debug(f"Message published (MID: {mid}, Reason: {reason_code})")

    def _handle_critical_message(self, client, data, ai_priority, ai_analysis, rule_priority, action):
        """Process critical priority messages"""
        selected_sub = self.select_subscriber(self.active_critical_subs)
        subscriber_num = selected_sub.split('subscriber')[-1]
        self.update_load(selected_sub, +1)
        
        self.logger.info(f"{Fore.RED}CRITICAL ALERT! Routing to {selected_sub}{Style.RESET_ALL}")
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

    def _handle_normal_message(self, data, ai_priority, rule_priority):
        """Process normal priority messages"""
        self.normal_batch.append({
            **data,
            "ai_priority": "normal",
            "ai_analysis": f"AI:{ai_priority}, Rule:{rule_priority}",
            "received_at": time.strftime("%Y-%m-%d %H:%M:%S")
        })
        
        self.logger.info(f"Added to normal batch (size: {len(self.normal_batch)})")
        
        if not self.batch_timer:
            self._reset_batch_timer()
        if len(self.normal_batch) >= self.batch_size:
            self.process_normal_batch()

    def select_subscriber(self, subscriber_pool):
        """Select a subscriber using weighted random selection"""
        total_weight = sum(sub['weight'] for sub in subscriber_pool.values())
        selection = random.uniform(0, total_weight)
        
        current = 0
        for sub_name, sub_info in subscriber_pool.items():
            adjusted_weight = max(1, sub_info['weight'] - sub_info['current_load'])
            current += adjusted_weight
            if selection <= current:
                return sub_name
        
        return next(iter(subscriber_pool.keys()))  # fallback

    def update_load(self, sub_name: str, change: int):
        """Update the load for a specific subscriber"""
        pool = self.active_critical_subs if "critical" in sub_name else self.active_normal_subs
            
        if sub_name in pool:
            pool[sub_name]['current_load'] += change
            pool[sub_name]['current_load'] = max(0, pool[sub_name]['current_load'])

    def ai_analyze(self, data) -> tuple:
        """Analyze energy data using AI with rate limit handling"""
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
                temperature=0.1,
                max_tokens=100,
                timeout=10  # Add timeout to prevent hanging
            )
            return self._parse_ai_response(response.choices[0].message.content)
        except Exception as e:
            self.logger.error(f"{Fore.RED}AI analysis failed: {str(e)}{Style.RESET_ALL}")
            raise  # Let the caller handle the exception

    def _parse_ai_response(self, response: str) -> tuple:
        """Parse the AI response into priority and analysis"""
        try:
            # Clean and normalize the response
            response = response.strip().lower()
            
            # Check for the expected format
            if "|" in response:
                priority, analysis = response.split("|", 1)
                priority = priority.strip()
                analysis = analysis.strip()
            else:
                # Fallback: look for priority keywords
                if "critical" in response:
                    priority = "critical"
                    analysis = response
                else:
                    priority = "normal"
                    analysis = response
            
            # Validate priority
            if priority not in ['normal', 'critical']:
                priority = 'normal'
                
            return (priority, analysis)
        except Exception as e:
            self.logger.warning(f"{Fore.YELLOW}Failed to parse AI response: {str(e)}{Style.RESET_ALL}")
            return ("normal", "AI analysis parsing failed")

    def process_normal_batch(self):
        """Process a batch of normal messages"""
        if not self.normal_batch:
            return
            
        self.logger.info(f"Processing batch of {len(self.normal_batch)} normal messages")
        self.logger.debug(f"Batch contents: {json.dumps(batch_data, indent=2)}")
        
        selected_sub = self.select_subscriber(self.active_normal_subs)
        subscriber_num = selected_sub.split('subscriber')[-1]
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
            f"energy/normal/subscriber{subscriber_num}",
            payload=json.dumps(batch_data),
            qos=1
        )
        
        Timer(2, self.update_load, args=(selected_sub, -1)).start()
        self.normal_batch.clear()
        self._reset_batch_timer()

    def _reset_batch_timer(self):
        """Reset the batch processing timer"""
        if self.batch_timer:
            self.batch_timer.cancel()
        self.batch_timer = Timer(self.batch_interval, self.process_normal_batch)
        self.batch_timer.start()

    def start(self):
        """Start the broker main loop"""
        try:
            self._print_startup_banner()
            self.client.loop_forever()
            
        except KeyboardInterrupt:
            self.logger.info(f"{Fore.YELLOW}🛑 Graceful shutdown initiated{Style.RESET_ALL}")
            self._cleanup()
        except Exception as e:
            self.logger.critical(f"{Fore.RED}⛔ Broker crashed: {str(e)}{Style.RESET_ALL}")
            self._cleanup()
            raise

    def _print_startup_banner(self):
        """Print the startup information banner"""
        self.logger.info(f"{Fore.GREEN}🚀 Starting AI-Enabled MQTT Broker with Auto-Scaling{Style.RESET_ALL}")
        self.logger.info(f"Initial Thresholds: "
                        f"High Power: {self.rule_manager.rules['high_power']['conditions'][0]['value']}W, "
                        f"High Temp: {self.rule_manager.rules['high_temp']['conditions'][0]['value']}°C")
        self.logger.info(f"Auto-Scaling Configuration: "
                        f"Normal: {len(self.active_normal_subs)}/{self.max_normal_subscribers}, "
                        f"Critical: {len(self.active_critical_subs)}/{self.max_critical_subscribers}")

    def _cleanup(self):
        """Cleanup resources before shutdown"""
        self.logger.info("Cleaning up resources...")
        
        # Cancel timers
        if self.batch_timer:
            self.batch_timer.cancel()
        if self.scaling_timer:
            self.scaling_timer.cancel()
        
        # Terminate subscribers
        self._terminate_subscribers(self.active_normal_subs)
        self._terminate_subscribers(self.active_critical_subs)
        
        # Disconnect MQTT
        self.client.disconnect()
        self.logger.info(f"{Fore.GREEN}Cleanup complete{Style.RESET_ALL}")

    def _terminate_subscribers(self, pool: dict):
        """Terminate all subscriber processes in a pool"""
        for name, info in list(pool.items()):
            if info['process']:
                try:
                    info['process'].terminate()
                    info['process'].wait(timeout=5)
                    self.logger.info(f"Terminated {name}")
                except Exception as e:
                    self.logger.error(f"{Fore.RED}Failed to terminate {name}: {str(e)}{Style.RESET_ALL}")

if __name__ == "__main__":
    try:
        broker = AIEnergyBroker("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")
        broker.start()
    except Exception as e:
        logging.critical(f"{Fore.RED}Failed to start broker: {str(e)}{Style.RESET_ALL}")
        sys.exit(1)
