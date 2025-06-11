import paho.mqtt.client as mqtt
from openai import OpenAI
import json
from colorama import Fore, Style
import time
from collections import deque, defaultdict # Added defaultdict for RuleManager stats
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
    # MODIFIED: Added ai_enabled parameter to __init__
    def __init__(self, groq_key: str, ai_enabled: bool = True):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"{Fore.YELLOW}Initializing AIEnergyBroker...{Style.RESET_ALL}")
        
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.critical_queue = deque()
        self.normal_batch = deque()
        self.metrics = MetricsTracker()
        self._message_tracker = {}
        self.ai_enabled = ai_enabled # MODIFIED: Set ai_enabled based on constructor argument
        self.ai_cache = {}
        self.dynamic_batch_size = 5
        self.batch_size_history = deque(maxlen=10)
        self.metrics_buffer = []
        self.metrics_timer = None
        
        self._init_database()
        
        # Initialize LLM client only if AI is enabled
        if self.ai_enabled:
            self.llm_client = OpenAI(
                base_url="https://api.groq.com/openai/v1",
                api_key=groq_key,
                max_retries=2
            )
        else:
            self.llm_client = None # Ensure it's None if AI is disabled
            
        self.batch_interval = 10
        self.batch_timer = None
        
        self.rule_manager = RuleManager()
        
        self.scaling_enabled = True
        self.max_normal_subscribers = 4
        self.max_critical_subscribers = 3
        self.min_normal_subscribers = 1
        self.min_critical_subscribers = 1
        self.scaling_interval = 60
        self.scaling_threshold_up = 0.85
        self.scaling_threshold_down = 0.15
        self.scaling_timer = None
        
        self.active_normal_subs = {}
        self.active_critical_subs = {}
        self._initialize_subscribers()
        
        self.monitor_thread = Thread(
            target=self._monitor_subscribers,
            daemon=True
        )
        self.monitor_thread.start()
        
        self.client = mqtt.Client("AIEnergyBroker", protocol=mqtt.MQTTv5)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        self._connect_mqtt()
        
        if self.scaling_enabled:
            self._start_scaling_engine()
            
    def _connect_mqtt(self):
        try:
            self.client.connect("localhost", 1883, 60)
            self.logger.info(f"{Fore.YELLOW}Connecting to MQTT broker...{Style.RESET_ALL}")
        except Exception as e:
            self.logger.error(f"{Fore.RED}MQTT connection error: {str(e)}{Style.RESET_ALL}")
            raise

    def _on_disconnect(self, client, userdata, rc, *args):
        if rc != 0:
            self.logger.warning(f"{Fore.YELLOW}Unexpected MQTT disconnection. Will auto-reconnect{Style.RESET_ALL}")
        else:
            self.logger.info(f"{Fore.GREEN}Disconnected from MQTT broker{Style.RESET_ALL}")

    def _on_subscribe(self, client, userdata, mid, granted_qos, *args):
        self.logger.debug(f"Subscribed to topic (MID: {mid}, QoS: {granted_qos})")
            
    def start(self):
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
        try:
            # subprocess.Popen([sys.executable, f'{sub_type}_subscriber.py', sub_id]) # Removed actual subprocess call for stability in environment
            pool[sub_name] = {
                'process': None, # Placeholder for process, assuming external management or mock
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
        conn = sqlite3.connect('performance.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS test_runs
                     (timestamp TEXT, test_case TEXT, throughput REAL, 
                      avg_latency REAL, accuracy REAL, subscriber_loads TEXT,
                      ai_enabled INTEGER)''')
        conn.commit()
        conn.close()

    def _monitor_subscribers(self):
        while True:
            try:
                for pool in [self.active_normal_subs, self.active_critical_subs]:
                    for sub_name, sub_info in pool.items():
                        # Mock load for demonstration; in real system, this would be from actual subscriber metrics
                        sub_info['current_load'] = random.uniform(0.1, 0.8) 
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"Monitoring error: {str(e)}")
                time.sleep(10)

    def _initialize_subscribers(self):
        try:
            for i in range(1, 3): # Initialize 2 normal subscribers
                sub_name = f"normal/subscriber{i}"
                self._start_and_register_subscriber("normal", str(i), sub_name, self.active_normal_subs)
            
            for i in range(1, 3): # Initialize 2 critical subscribers
                sub_name = f"critical/subscriber{i}"
                self._start_and_register_subscriber("critical", str(i), sub_name, self.active_critical_subs)
                
        except Exception as e:
            self.logger.error(f"{Fore.RED}Failed to initialize subscribers: {str(e)}{Style.RESET_ALL}")
            raise

    def _on_message(self, client, userdata, msg):
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

            # Update RuleManager stats with the new message here,
            # so historical data is available for AI before _process_message is called
            self.rule_manager.update_stats(data)
            self.rule_manager.adapt_rules() # Adapt rules based on new data

            # No need to evaluate rule_priority here and then pass it to self._process_message,
            # _process_message will handle the full routing logic including AI.
            self._process_message(data)
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            emergency_msg = {
                'error': str(e),
                'original_topic': msg.topic,
                'timestamp': datetime.now().isoformat()
            }
            # Fallback to critical routing if any processing error occurs
            self._handle_critical_message(
                client=self.client,
                data=emergency_msg,
                ai_priority="critical", # Assume critical if processing fails
                ai_analysis="Processing error in broker, routed as critical",
                rule_priority="critical", # Assume critical if processing fails
                action="emergency_route"
            )
        finally:
            processing_time = time.time() - start_time
            self.metrics.log_processing_time(processing_time)

    def _on_publish(self, client, userdata, mid):
        self.logger.debug(f"Message published (MID: {mid})")
        message_id = str(uuid.uuid4())
        self._message_tracker[message_id] = {
            "sent_time": time.time(),
            "status": "published"
        }

    def store_results(self, test_case, results):
        self._buffer_metrics({
            "test_case": test_case,
            "results": results
        })

    def _buffer_metrics(self, data):
        self.metrics_buffer.append(data)
        if len(self.metrics_buffer) >= 10 or (self.metrics_timer is None and self.metrics_buffer):
            self._flush_metrics()
        elif not self.metrics_timer:
            self.metrics_timer = Timer(5.0, self._flush_metrics)
            self.metrics_timer.start()

    def _flush_metrics(self):
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
        self._check_scaling_needs()
        self.scaling_timer = Timer(self.scaling_interval, self._start_scaling_engine)
        self.scaling_timer.daemon = True
        self.scaling_timer.start()
        self.logger.info(f"{Fore.CYAN}Auto-scaling engine started (interval: {self.scaling_interval}s){Style.RESET_ALL}")

    def _check_scaling_needs(self):
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
            # When scaling down, identify a subscriber to remove.
            # A more sophisticated approach would consider current load/idle time.
            # Here, we just remove the last one added.
            sub_to_remove_name = None
            for name, info in list(pool.items()):
                if info['type'] == sub_type: # Ensure we only pick from the correct type
                    sub_to_remove_name = name
                    break # Take the first one for simplicity, can be improved

            if sub_to_remove_name:
                # If there's an actual process, terminate it gracefully
                if pool[sub_to_remove_name]['process']:
                    try:
                        pool[sub_to_remove_name]['process'].terminate()
                        pool[sub_to_remove_name]['process'].wait(timeout=5)
                        self.logger.info(f"Terminated process for {sub_to_remove_name}")
                    except Exception as e:
                        self.logger.error(f"Failed to terminate process for {sub_to_remove_name}: {str(e)}")
                del pool[sub_to_remove_name]
                self.logger.info(f"{Fore.YELLOW}Scaled {sub_type} subscribers {direction} to {len(pool)}{Style.RESET_ALL}")
            else:
                self.logger.warning(f"No {sub_type} subscriber found to scale down.")


    def _process_message(self, data):
        try:
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().isoformat()
            
            ai_priority = "normal"
            ai_analysis = "AI disabled or unavailable"

            # MODIFIED: Conditionally call AI analysis
            if self.ai_enabled and self.llm_client:
                try:
                    # Pass the rule_manager's message_stats to the AI for historical context
                    ai_priority, ai_analysis = self.ai_analyze(data)
                    self.logger.info(f"AI Analysis - Priority: {ai_priority}, Analysis: {ai_analysis}")
                except Exception as e:
                    self.logger.warning(f"AI analysis failed: {str(e)}")
                    ai_priority = "normal"
                    ai_analysis = f"AI analysis failed: {str(e)}"
            else:
                self.logger.info("AI analysis skipped (AI disabled or client not initialized)")
                
            rule_priority, action = self.rule_manager.evaluate_message(data)
            
            # Decision logic: if AI is critical OR rules are critical, route as critical
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
        selected_sub = self.select_subscriber(self.active_critical_subs)
        # Ensure selected_sub is not None or empty
        if not selected_sub:
            self.logger.error("No critical subscriber available for routing.")
            return

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
        if not subscriber_pool:
            # Handle case where no subscribers are available
            return None 
        
        weighted_subs = []
        for sub_name, sub_info in subscriber_pool.items():
            weight = sub_info.get('weight', 1.0)
            load_factor = 1.0 - sub_info['current_load']
            perf_factor = 1.0 / (sub_info.get('avg_process_time', 1.0) + 0.1)
            combined_weight = weight * load_factor * perf_factor
            weighted_subs.append((sub_name, combined_weight))
        
        total_weight = sum(w for _, w in weighted_subs)
        if total_weight <= 0:
            # If all weights are zero or negative, pick any available subscriber
            return next(iter(subscriber_pool.keys()))
        
        selection = random.uniform(0, total_weight)
        current = 0
        for sub_name, weight in weighted_subs:
            current += weight
            if selection <= current:
                return sub_name
        
        return next(iter(subscriber_pool.keys())) # Fallback, should not be reached if total_weight > 0

    def update_load(self, sub_name: str, change: int):
        pool = self.active_critical_subs if "critical" in sub_name else self.active_normal_subs
            
        if sub_name in pool:
            pool[sub_name]['current_load'] += change
            pool[sub_name]['current_load'] = max(0, pool[sub_name]['current_load'])

    # ai_analyze now passes message_stats to _perform_ai_analysis
    def ai_analyze(self, data) -> tuple:
        # Use a more comprehensive cache key that includes a simplified history fingerprint if possible
        # For this example, we'll keep it simple, but in a real system, you might hash parts of the history
        cache_key = (
            round(data['energy']['total']),
            round(data['energy']['lights']),
            tuple(sorted((k, round(v['temperature'])) for k,v in data['zones'].items())),
            # You might add a hash of a recent slice of energy_history if it were part of the cache key
            # hash(frozenset(self.rule_manager.message_stats['energy_total'])) 
        )
        
        if cache_key in self.ai_cache:
            self.metrics.cache_hits += 1
            if self.metrics.cache_hits % 100 == 0:
                self.logger.info(f"AI Cache hits: {self.metrics.cache_hits}")
            return self.ai_cache[cache_key]
        
        # Pass the rule_manager's message_stats to the AI for historical context
        result = self._perform_ai_analysis(data, self.rule_manager.message_stats)
        
        if len(self.ai_cache) > 1000: # Simple cache eviction
            self.ai_cache.pop(next(iter(self.ai_cache)))
        self.ai_cache[cache_key] = result
        
        return result

    # _perform_ai_analysis now accepts message_stats
    def _perform_ai_analysis(self, data, message_stats: defaultdict) -> tuple:
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
        
        # --- Prepare historical summary for the AI prompt ---
        historical_summary = {}
        
        # Power history summary
        if 'energy_total' in message_stats and len(message_stats['energy_total']) >= 2:
            historical_total_power = list(message_stats['energy_total']) # Convert deque to list
            historical_summary['avg_power_last_100'] = f"{statistics.mean(historical_total_power):.2f}W"
            historical_summary['std_dev_power_last_100'] = f"{statistics.stdev(historical_total_power):.2f}W"
            historical_summary['min_power_last_100'] = f"{min(historical_total_power)}W"
            historical_summary['max_power_last_100'] = f"{max(historical_total_power)}W"
            historical_summary['recent_power_values'] = [f"{p}W" for p in historical_total_power[-10:]] # Last 10 values
        else:
            historical_summary['power_history'] = "Insufficient historical power data (less than 2 points)."

        # Temperature history summary for each zone
        historical_temps_summary = {}
        for key, temps in message_stats.items():
            if key.startswith('temp_') and len(temps) >= 2:
                zone_name = key.replace('temp_', '').capitalize()
                historical_temps_summary[zone_name] = {
                    'avg_temp_last_100': f"{statistics.mean(temps):.2f}°C",
                    'std_dev_temp_last_100': f"{statistics.stdev(temps):.2f}°C",
                    'min_temp_last_100': f"{min(temps)}°C",
                    'max_temp_last_100': f"{max(temps)}°C",
                    'recent_temp_values': [f"{t}°C" for t in list(temps)[-10:]] # Last 10 values
                }
            elif key.startswith('temp_'):
                historical_temps_summary[key.replace('temp_', '').capitalize()] = "Insufficient historical temperature data."

        if historical_temps_summary:
            historical_summary['zones_temp_history'] = historical_temps_summary
        else:
            historical_summary['zones_temp_history'] = "No historical zone temperature data."
        # --- END NEW LOGIC ---

        # Construct the AI prompt to include historical context and guide pattern anomaly detection
        prompt = f"""
            System: You are an expert AI for industrial energy system anomaly detection.
            Your primary goal is to identify **critical anomalies** based on current data, **its deviation from historical patterns**, and predefined alert thresholds.
            Consider both explicit threshold breaches and **subtle pattern changes** that indicate an issue.
            Output your decision as 'priority|brief_reason'.

            Strict Critical Conditions:
            1.  **High Power Alert:** If 'total_power' is significantly above the 'Power Alert Threshold' (e.g., > threshold * 1.1).
            2.  **Low Power (Potential Failure):** If 'total_power' is less than 50W (suggests equipment failure or severe underutilization).
            3.  **High Temperature Alert:** If ANY zone's temperature is significantly above the 'Temperature Alert Threshold' (e.g., > threshold * 1.1).
            4.  **Significant Deviation from Historical Norm:** If the current 'total_power' or any zone's temperature is statistically unusual compared to its `avg_power_last_100`/`avg_temp_last_100` and `std_dev_power_last_100`/`std_dev_temp_last_100`. For instance, if a value is more than 3 standard deviations away from the average, it's highly anomalous, even if it's within the general threshold.
            5.  **Pattern Break Anomaly:** Look for patterns that are highly unusual for the current time or sequence, even if they don't break immediate thresholds. For example, a sudden drop in power during peak hours, or a gradual but consistent rise in temperature over several hours that accelerates.
            6.  **Equipment Malfunction Indication:** Look for combinations like HVAC power being zero while temperatures are rising in multiple zones, or lights/equipment power showing highly erratic behavior.

            Otherwise, classify as 'normal'.

            User:
            CURRENT ENERGY DATA:
            {json.dumps(energy_data, indent=2)}

            CURRENT THRESHOLDS (Rule-based, dynamically adapted):
            - Power Alert Threshold: > {current_power_threshold:.1f}W
            - Temperature Alert Threshold: > {current_temp_threshold:.1f}°C

            HISTORICAL SUMMARY (last 100 data points, for pattern comparison):
            {json.dumps(historical_summary, indent=2)}

            Based on the CURRENT ENERGY DATA and the HISTORICAL SUMMARY, is this state critical?
            """
        
        try:
            response = self.llm_client.chat.completions.create(
                model="llama3-70b-8192", # Using 70B for more nuanced reasoning
                messages=[
                    {"role": "system", "content": "You are an expert AI for industrial energy system anomaly detection. Your task is to analyze the provided energy data in the context of historical patterns and predefined thresholds. Determine if the current state is 'critical' or 'normal' and provide a brief reason."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1, # Keep it deterministic for anomaly detection
                max_tokens=250,  # Increased max_tokens for detailed analysis
                timeout=20       # Increased timeout for potentially longer AI analysis
            )
            return self._parse_ai_response(response.choices[0].message.content)
        except Exception as e:
            self.logger.warning(f"AI API call failed: {str(e)}")
            return "normal", f"AI analysis unavailable due to API error: {str(e)}"

    def _parse_ai_response(self, response: str) -> tuple:
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
                # Fallback if AI provides an unexpected priority
                self.logger.warning(f"AI returned invalid priority '{priority}'. Defaulting to 'normal'.")
                priority = 'normal'
                
            return (priority, analysis)
        except Exception as e:
            self.logger.warning(f"{Fore.YELLOW}Failed to parse AI response: {str(e)}{Style.RESET_ALL}")
            return ("normal", "AI analysis parsing failed")

    def process_normal_batch(self):
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
        # Ensure selected_sub is not None or empty
        if not selected_sub:
            self.logger.error("No normal subscriber available for batch routing.")
            return

        subscriber_num = selected_sub.split('subscriber')[-1]
        self.update_load(selected_sub, +1)
        
        batch_data = {
            "messages": batch_messages,
            "batch_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "batch_size": len(batch_messages),
            "ai_priority": "normal", # AI analysis for batch is always normal here
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
        if self.batch_timer:
            self.batch_timer.cancel()
        self.batch_timer = Timer(self.batch_interval, self.process_normal_batch)
        self.batch_timer.start()

    def _cleanup(self):
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
        for name, info in list(pool.items()):
            if info['process']:
                try:
                    info['process'].terminate()
                    info['process'].wait(timeout=5)
                    self.logger.info(f"Terminated {name}")
                except Exception as e:
                    self.logger.error(f"{Fore.RED}Failed to terminate {name}: {str(e)}{Style.RESET_ALL}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AI-Enabled MQTT Broker for Energy Management")
    parser.add_argument("--disable-ai", action="store_true", help="Disable AI analysis for messages.")
    args = parser.parse_args()

    try:
        # Pass the ai_enabled flag to the broker constructor
        # Replace "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" with your actual Groq API key
        broker = AIEnergyBroker(
            groq_key="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",
            ai_enabled=not args.disable_ai
        )
        broker.start()
    except Exception as e:
        logging.critical(f"{Fore.RED}Failed to start broker: {str(e)}{Style.RESET_ALL}")
        sys.exit(1)

