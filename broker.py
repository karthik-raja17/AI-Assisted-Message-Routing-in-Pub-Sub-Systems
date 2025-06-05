#!/usr/bin/env python3
"""
AI-Enabled MQTT Energy Broker with Auto-Scaling Subscribers (Optimized)
"""

import paho.mqtt.client as mqtt
from openai import OpenAI
import json
from colorama import Fore, Style
import time
from collections import deque
from threading import Timer, Thread
from rule_manager import RuleManager
import random
import subprocess
import sys
import logging
import traceback
from datetime import datetime
import sqlite3
import uuid
import statistics
from concurrent.futures import ThreadPoolExecutor

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

class MetricsTracker:
    def __init__(self):
        self.processing_times = []
        self.message_tracker = {}
        self.routing_decisions = {"correct": 0, "incorrect": 0}
        self.cache_hits = 0
        
    def log_processing_time(self, time):
        self.processing_times.append(time)
        
    def log_routing_decision(self, correct):
        if correct:
            self.routing_decisions["correct"] += 1
        else:
            self.routing_decisions["incorrect"] += 1
            
    def get_stats(self):
        return {
            "avg_processing_time": statistics.mean(self.processing_times) if self.processing_times else 0,
            "routing_accuracy": self.routing_decisions["correct"] / sum(self.routing_decisions.values()) if sum(self.routing_decisions.values()) > 0 else 1,
            "cache_hits": self.cache_hits
        }

class AIEnergyBroker:
    def __init__(self, groq_key: str):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"{Fore.YELLOW}Initializing AIEnergyBroker...{Style.RESET_ALL}")
        
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.critical_queue = deque()
        self.normal_batch = deque()
        self.metrics = MetricsTracker()
        self._message_tracker = {}
        self.ai_enabled = True
        self.ai_cache = {}
        self.dynamic_batch_size = 5
        self.batch_size_history = deque(maxlen=10)
        self.metrics_buffer = []
        self.metrics_timer = None
        
        # Initialize database
        self._init_database()
        
        # Initialize AI Client
        self.llm_client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_key,
            max_retries=2
        )
        
        # Batch processing configuration
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
        self.scaling_interval = 60
        self.scaling_threshold_up = 0.85
        self.scaling_threshold_down = 0.15
        self.scaling_timer = None
        
        # Current active subscribers
        self.active_normal_subs = {}
        self.active_critical_subs = {}
        self._initialize_subscribers()
        
        # Start monitoring thread
        self.monitor_thread = Thread(
            target=self._monitor_subscribers,
            daemon=True
        )
        self.monitor_thread.start()
        
        # MQTT Client setup
        self.client = mqtt.Client("AIEnergyBroker", protocol=mqtt.MQTTv5)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        self._connect_mqtt()
        
        # Start scaling monitor
        if self.scaling_enabled:
            self._start_scaling_engine()
            
    def _connect_mqtt(self):
        """Connect to the MQTT broker"""
        try:
            self.client.connect("localhost", 1883, 60)
            self.logger.info(f"{Fore.YELLOW}Connecting to MQTT broker...{Style.RESET_ALL}")
        except Exception as e:
            self.logger.error(f"{Fore.RED}MQTT connection error: {str(e)}{Style.RESET_ALL}")
            raise

    def _on_disconnect(self, client, userdata, rc, *args):
        """Callback for when the client disconnects from the broker"""
        if rc != 0:
            self.logger.warning(f"{Fore.YELLOW}Unexpected MQTT disconnection. Will auto-reconnect{Style.RESET_ALL}")
        else:
            self.logger.info(f"{Fore.GREEN}Disconnected from MQTT broker{Style.RESET_ALL}")

    def _on_subscribe(self, client, userdata, mid, granted_qos, *args):
        """Callback for when the client subscribes to a topic"""
        self.logger.debug(f"Subscribed to topic (MID: {mid}, QoS: {granted_qos})")
            
    def start(self):
        """Start the MQTT client loop"""
        try:
            self._print_startup_banner()
            self.client.loop_forever()
        except KeyboardInterrupt:
            self.logger.info(f"{Fore.YELLOW}Shutting down broker...{Style.RESET_ALL}")
            self.client.disconnect()
        except Exception as e:
            self.logger.critical(f"{Fore.RED}Broker crashed: {str(e)}{Style.RESET_ALL}")
            raise
    
    def _start_and_register_subscriber(self, sub_type: str, sub_id: str, sub_name: str, pool: dict):
        """Start a subscriber process and register it in the active pool"""
        try:
            pool[sub_name] = {
                'process': None,
                'start_time': time.time(),
                'current_load': 0.0,
                'message_count': 0,
                'type': sub_type,
                'id': sub_id,
                'weight': 1.0,
                'avg_process_time': 0.1
            }
            self.logger.info(f"{Fore.GREEN}Started {sub_type} subscriber {sub_id}{Style.RESET_ALL}")
        except Exception as e:
            self.logger.error(f"{Fore.RED}Failed to start {sub_type} subscriber {sub_id}: {str(e)}{Style.RESET_ALL}")
            raise
    
    def _on_connect(self, client, userdata, flags, rc, *args):
        """Callback for when the client receives a CONNACK response from the server"""
        if rc == 0:
            self.logger.info(f"{Fore.GREEN}Connected to MQTT broker successfully{Style.RESET_ALL}")
            self.client.subscribe("building/energy")
        else:
            error_msg = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorized"
            }.get(rc, f"Unknown error code {rc}")
            self.logger.error(f"{Fore.RED}Failed to connect to MQTT broker: {error_msg}{Style.RESET_ALL}")
            raise ConnectionError(f"MQTT connection failed: {error_msg}")

    def _init_database(self):
        """Initialize the performance database"""
        conn = sqlite3.connect('performance.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS test_runs
                     (timestamp TEXT, test_case TEXT, throughput REAL, 
                      avg_latency REAL, accuracy REAL, subscriber_loads TEXT,
                      ai_enabled INTEGER)''')
        conn.commit()
        conn.close()

    def _monitor_subscribers(self):
        """Continuously monitor subscriber loads"""
        while True:
            try:
                for pool in [self.active_normal_subs, self.active_critical_subs]:
                    for sub_name, sub_info in pool.items():
                        sub_info['current_load'] = random.uniform(0.1, 0.8)
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Monitoring error: {str(e)}")
                time.sleep(10)

    def _initialize_subscribers(self):
        """Initialize default subscriber processes"""
        try:
            for i in range(1, 3):
                sub_name = f"normal/subscriber{i}"
                self._start_and_register_subscriber("normal", str(i), sub_name, self.active_normal_subs)
            
            for i in range(1, 3):
                sub_name = f"critical/subscriber{i}"
                self._start_and_register_subscriber("critical", str(i), sub_name, self.active_critical_subs)
                
        except Exception as e:
            self.logger.error(f"{Fore.RED}Failed to initialize subscribers: {str(e)}{Style.RESET_ALL}")
            raise

    def _on_message(self, client, userdata, msg):
        """Safe message handling with full error protection"""
        start_time = time.time()
        try:
            if not msg.payload:
                self.logger.error("Received empty message payload")
                return
            
            self.executor.submit(self._process_message_async, msg, start_time)
        except Exception as e:
            self.logger.error(f"Error submitting to thread pool: {str(e)}")

    def _process_message_async(self, msg, start_time):
        try:
            data = json.loads(msg.payload.decode('utf-8'))
            if not all(field in data for field in ['energy', 'zones']):
                self.logger.error("Missing required fields")
                return

            rule_priority, _ = self.rule_manager.evaluate_message(data)
            if rule_priority == "critical" or data['energy']['total'] > self.rule_manager.rules['high_power']['conditions'][0]['value'] * 0.8:
                self.critical_queue.appendleft(data)
            else:
                self.normal_batch.append(data)

            self._process_message(data)
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            emergency_msg = {
                'error': str(e),
                'original_topic': msg.topic,
                'timestamp': datetime.now().isoformat()
            }
            self._handle_critical_message(
                client=self.client,
                data=emergency_msg,
                ai_priority="critical",
                ai_analysis="Processing error",
                rule_priority="critical",
                action="emergency_route"
            )
        finally:
            processing_time = time.time() - start_time
            self.metrics.log_processing_time(processing_time)

    def _on_publish(self, client, userdata, mid):
        """Handle publish confirmation"""
        self.logger.debug(f"Message published (MID: {mid})")
        message_id = str(uuid.uuid4())
        self._message_tracker[message_id] = {
            "sent_time": time.time(),
            "status": "published"
        }

    def store_results(self, test_case, results):
        """Store test results in the database"""
        self._buffer_metrics({
            "test_case": test_case,
            "results": results
        })

    def _buffer_metrics(self, data):
        """Buffer metrics for batch database writes"""
        self.metrics_buffer.append(data)
        if len(self.metrics_buffer) >= 10 or (self.metrics_timer is None and self.metrics_buffer):
            self._flush_metrics()
        elif not self.metrics_timer:
            self.metrics_timer = Timer(5.0, self._flush_metrics)
            self.metrics_timer.start()

    def _flush_metrics(self):
        """Flush buffered metrics to database"""
        if self.metrics_timer:
            self.metrics_timer.cancel()
            self.metrics_timer = None
        
        if not self.metrics_buffer:
            return
        
        conn = sqlite3.connect('performance.db')
        c = conn.cursor()
        try:
            c.executemany('''INSERT INTO test_runs VALUES 
                            (?,?,?,?,?,?,?)''', [
                (datetime.now().isoformat(),
                 data['test_case']['name'],
                 data['results']['throughput'],
                 data['results']['avg_latency'],
                 data['results']['accuracy'],
                 json.dumps(data['results']['subscriber_loads']),
                 int(self.ai_enabled))
                for data in self.metrics_buffer
            ])
            conn.commit()
            self.metrics_buffer.clear()
        except Exception as e:
            self.logger.error(f"Metrics flush failed: {str(e)}")
        finally:
            conn.close()

    def _print_startup_banner(self):
        """Print the startup information banner"""
        self.logger.info(f"{Fore.GREEN}🚀 Starting AI-Enabled MQTT Broker with Auto-Scaling{Style.RESET_ALL}")
        self.logger.info(f"Initial Thresholds: "
                        f"High Power: {self.rule_manager.rules['high_power']['conditions'][0]['value']}W, "
                        f"High Temp: {self.rule_manager.rules['high_temp']['conditions'][0]['value']}°C")
        self.logger.info(f"Auto-Scaling Configuration: "
                        f"Normal: {len(self.active_normal_subs)}/{self.max_normal_subscribers}, "
                        f"Critical: {len(self.active_critical_subs)}/{self.max_critical_subscribers}")
        self.logger.info(f"Performance Monitoring: Enabled")
        self.logger.info(f"AI Routing: {'Enabled' if self.ai_enabled else 'Disabled'}")
        
    def _start_scaling_engine(self):
        """Start the auto-scaling timer"""
        self._check_scaling_needs()
        self.scaling_timer = Timer(self.scaling_interval, self._start_scaling_engine)
        self.scaling_timer.daemon = True
        self.scaling_timer.start()
        self.logger.info(f"{Fore.CYAN}Auto-scaling engine started (interval: {self.scaling_interval}s){Style.RESET_ALL}")

    def _check_scaling_needs(self):
        """Check if subscriber scaling is needed"""
        try:
            normal_load = statistics.mean([sub['current_load'] for sub in self.active_normal_subs.values()]) if self.active_normal_subs else 0
            critical_load = statistics.mean([sub['current_load'] for sub in self.active_critical_subs.values()]) if self.active_critical_subs else 0

            if normal_load > self.scaling_threshold_up and len(self.active_normal_subs) < self.max_normal_subscribers:
                self._scale_subscribers('normal', 'up')
            elif normal_load < self.scaling_threshold_down and len(self.active_normal_subs) > self.min_normal_subscribers:
                self._scale_subscribers('normal', 'down')

            if critical_load > self.scaling_threshold_up and len(self.active_critical_subs) < self.max_critical_subscribers:
                self._scale_subscribers('critical', 'up')
            elif critical_load < self.scaling_threshold_down and len(self.active_critical_subs) > self.min_critical_subscribers:
                self._scale_subscribers('critical', 'down')

        except Exception as e:
            self.logger.error(f"{Fore.RED}Scaling check failed: {str(e)}{Style.RESET_ALL}")

    def _scale_subscribers(self, sub_type: str, direction: str):
        """Scale subscribers up or down"""
        pool = self.active_normal_subs if sub_type == 'normal' else self.active_critical_subs
        current_count = len(pool)
        max_count = self.max_normal_subscribers if sub_type == 'normal' else self.max_critical_subscribers
        min_count = self.min_normal_subscribers if sub_type == 'normal' else self.min_critical_subscribers

        if direction == 'up' and current_count < max_count:
            new_id = str(current_count + 1)
            sub_name = f"{sub_type}/subscriber{new_id}"
            self._start_and_register_subscriber(sub_type, new_id, sub_name, pool)
            self.logger.info(f"{Fore.GREEN}Scaled {sub_type} subscribers {direction} to {len(pool)}{Style.RESET_ALL}")
        elif direction == 'down' and current_count > min_count:
            sub_to_remove = list(pool.keys())[-1]
            del pool[sub_to_remove]
            self.logger.info(f"{Fore.YELLOW}Scaled {sub_type} subscribers {direction} to {len(pool)}{Style.RESET_ALL}")

    def _process_message(self, data):
        """Central message processing handler"""
        try:
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().isoformat()
            
            self.rule_manager.update_stats(data)
            self.rule_manager.adapt_rules()
            
            try:
                ai_priority, ai_analysis = self.ai_analyze(data)
                self.logger.info(f"AI Analysis - Priority: {ai_priority}, Analysis: {ai_analysis}")
            except Exception as e:
                self.logger.warning(f"AI analysis failed, using fallback: {str(e)}")
                ai_priority = "normal"
                ai_analysis = "AI analysis unavailable"
            
            rule_priority, action = self.rule_manager.evaluate_message(data)
            
            if rule_priority == "critical" or ai_priority == "critical":
                self._handle_critical_message(
                    client=self.client,
                    data=data,
                    ai_priority=ai_priority,
                    ai_analysis=ai_analysis,
                    rule_priority=rule_priority,
                    action=action
                )
            else:
                self._handle_normal_message(data, ai_priority, rule_priority)
                
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            traceback.print_exc()
    
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
        if len(self.normal_batch) >= self.dynamic_batch_size:
            self.process_normal_batch()

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

    def select_subscriber(self, subscriber_pool):
        """Enhanced subscriber selection with complexity awareness"""
        if not subscriber_pool:
            raise ValueError("No subscribers available in pool")
        
        weighted_subs = []
        for sub_name, sub_info in subscriber_pool.items():
            weight = sub_info.get('weight', 1.0)
            load_factor = 1.0 - sub_info['current_load']
            perf_factor = 1.0 / (sub_info.get('avg_process_time', 1.0) + 0.1)
            combined_weight = weight * load_factor * perf_factor
            weighted_subs.append((sub_name, combined_weight))
        
        total_weight = sum(w for _, w in weighted_subs)
        if total_weight <= 0:
            return next(iter(subscriber_pool.keys()))
        
        selection = random.uniform(0, total_weight)
        current = 0
        for sub_name, weight in weighted_subs:
            current += weight
            if selection <= current:
                return sub_name
        
        return next(iter(subscriber_pool.keys()))

    def update_load(self, sub_name: str, change: int):
        """Update the load for a specific subscriber"""
        pool = self.active_critical_subs if "critical" in sub_name else self.active_normal_subs
            
        if sub_name in pool:
            pool[sub_name]['current_load'] += change
            pool[sub_name]['current_load'] = max(0, pool[sub_name]['current_load'])

    def ai_analyze(self, data) -> tuple:
        """Analyze energy data using AI with caching"""
        cache_key = (
            round(data['energy']['total']),
            round(data['energy']['lights']),
            tuple(sorted((k, round(v['temperature'])) for k,v in data['zones'].items())
        ))
        
        if cache_key in self.ai_cache:
            self.metrics.cache_hits += 1
            if self.metrics.cache_hits % 100 == 0:
                self.logger.info(f"AI Cache hits: {self.metrics.cache_hits}")
            return self.ai_cache[cache_key]
        
        result = self._perform_ai_analysis(data)
        
        if len(self.ai_cache) > 1000:
            self.ai_cache.pop(next(iter(self.ai_cache)))
        self.ai_cache[cache_key] = result
        
        return result

    def _perform_ai_analysis(self, data):
        """Perform actual AI analysis"""
        current_power_threshold = self.rule_manager.rules['high_power']['conditions'][0]['value']
        current_temp_threshold = self.rule_manager.rules['high_temp']['conditions'][0]['value']
        
        energy_data = {
            "total_power": data['energy']['total'],
            "lights_power": data['energy']['lights'],
            "equipment_power": data['energy'].get('equipment', 0),
            "hvac_power": data['energy'].get('hvac', 0),
            "zones": {zone: values['temperature'] for zone, values in data['zones'].items()},
            "current_thresholds": {
                "power": current_power_threshold,
                "temperature": current_temp_threshold
            }
        }
        
        prompt = f"""
        Analyze this industrial energy system data (STRICTLY FOLLOW FORMAT):

        CURRENT DATA:
        {json.dumps(energy_data, indent=2)}

        CURRENT THRESHOLDS:
        - Power Alert: > {current_power_threshold:.1f}W or < 100W
        - Temp Alert: > {current_temp_threshold:.1f}°C

        STRICT CRITICAL CONDITIONS:
        1. MUST report critical ONLY if:
           - Power > {current_power_threshold*1.1:.1f}W (10% buffer) OR < 50W
           - Any zone > {current_temp_threshold*1.1:.1f}°C (10% buffer)
           - Clear equipment failure patterns
        2. Otherwise report normal

        RESPONSE FORMAT:
        priority|analysis_summary
        """
        
        try:
            response = self.llm_client.chat.completions.create(
                model="llama3-70b-8192",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=100,
                timeout=5
            )
            return self._parse_ai_response(response.choices[0].message.content)
        except Exception as e:
            return "normal", "AI timeout"

    def _parse_ai_response(self, response: str) -> tuple:
        """Parse the AI response into priority and analysis"""
        try:
            response = response.strip().lower()
            
            if "|" in response:
                priority, analysis = response.split("|", 1)
                priority = priority.strip()
                analysis = analysis.strip()
            else:
                if "critical" in response:
                    priority = "critical"
                    analysis = response
                else:
                    priority = "normal"
                    analysis = response
            
            if priority not in ['normal', 'critical']:
                priority = 'normal'
                
            return (priority, analysis)
        except Exception as e:
            self.logger.warning(f"{Fore.YELLOW}Failed to parse AI response: {str(e)}{Style.RESET_ALL}")
            return ("normal", "AI analysis parsing failed")

    def process_normal_batch(self):
        """Dynamic batch processing based on system load"""
        if not self.normal_batch:
            return
            
        start_time = time.time()
        
        if len(self.batch_size_history) > 5:
            avg_process_time = statistics.mean(t for t in self.batch_size_history)
            if avg_process_time < 0.5:
                self.dynamic_batch_size = min(10, self.dynamic_batch_size + 1)
            else:
                self.dynamic_batch_size = max(2, self.dynamic_batch_size - 1)
        
        batch_messages = []
        while len(batch_messages) < self.dynamic_batch_size and self.normal_batch:
            batch_messages.append(self.normal_batch.popleft())
        
        selected_sub = self.select_subscriber(self.active_normal_subs)
        subscriber_num = selected_sub.split('subscriber')[-1]
        self.update_load(selected_sub, +1)
        
        batch_data = {
            "messages": batch_messages,
            "batch_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "batch_size": len(batch_messages),
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
        self.batch_size_history.append(time.time() - start_time)
        self._reset_batch_timer()

    def _reset_batch_timer(self):
        """Reset the batch processing timer"""
        if self.batch_timer:
            self.batch_timer.cancel()
        self.batch_timer = Timer(self.batch_interval, self.process_normal_batch)
        self.batch_timer.start()

    def _cleanup(self):
        """Cleanup resources before shutdown"""
        self.logger.info("Cleaning up resources...")
        
        if self.batch_timer:
            self.batch_timer.cancel()
        if self.scaling_timer:
            self.scaling_timer.cancel()
        
        self._terminate_subscribers(self.active_normal_subs)
        self._terminate_subscribers(self.active_critical_subs)
        
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
