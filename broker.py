import paho.mqtt.client as mqtt
from openai import OpenAI
import json
from colorama import Fore, Style
import time
from collections import deque
from threading import Timer
from rule_manager import RuleManager
import statistics

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
        
        # Initialize Rule Manager with lower initial thresholds
        self.rule_manager = RuleManager()
        
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

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"{Fore.GREEN}Broker connected to MQTT server (RC: {rc}){Style.RESET_ALL}")
            client.subscribe("building/energy", qos=1)
            print(f"{Fore.CYAN}Subscribed to 'building/energy' topic{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Broker connection failed (RC: {rc}){Style.RESET_ALL}")

    def ai_analyze(self, data) -> tuple:
        """Enhanced AI analysis with dynamic thresholds from RuleManager"""
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
                temperature=0.1  # More deterministic responses
            )
            return self._parse_ai_response(response.choices[0].message.content)
        except Exception as e:
            print(f"{Fore.RED}AI analysis failed: {str(e)}{Style.RESET_ALL}")
            return ("normal", "AI analysis failed")

    def _parse_ai_response(self, response: str) -> tuple:
        """Strict parsing with validation"""
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
        """Process accumulated normal messages"""
        if not self.normal_batch:
            return
            
        print(f"{Fore.CYAN}Processing batch of {len(self.normal_batch)} normal messages{Style.RESET_ALL}")
        
        batch_data = {
            "messages": list(self.normal_batch),
            "batch_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "batch_size": len(self.normal_batch),
            "ai_priority": "normal",
            "ai_analysis": "Batch processed normal messages"
        }
        
        self.client.publish(
            "energy/normal/batch",
            payload=json.dumps(batch_data),
            qos=1
        )
        
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
            
            # Update statistics and adapt rules
            self.rule_manager.update_stats(data)
            threshold_changed = self.rule_manager.adapt_rules()
            
            # Get evaluations
            ai_priority, ai_analysis = self.ai_analyze(data)
            rule_priority, action = self.rule_manager.evaluate_message(data)
            
            # Debug output
            print(f"\n{Fore.MAGENTA}=== Decision Analysis ==={Style.RESET_ALL}")
            print(f"Current Power: {current_power}W")
            print(f"AI Decision: {ai_priority} | Rule Decision: {rule_priority}")
            if threshold_changed:
                print(f"{Fore.YELLOW}Thresholds were updated this cycle{Style.RESET_ALL}")
            
            # Final decision - critical if either system detects issues
            final_priority = "critical" if ("critical" in [ai_priority, rule_priority]) else "normal"
            
            if final_priority == "critical":
                print(f"{Fore.RED}CRITICAL ALERT! Routing to red subscriber{Style.RESET_ALL}")
                client.publish(
                    "energy/critical",
                    payload=json.dumps({
                        **data,
                        "ai_priority": ai_priority,
                        "ai_analysis": ai_analysis,
                        "rule_priority": rule_priority,
                        "rule_trigger": action,
                        "current_threshold": self.rule_manager.rules['high_power']['conditions'][0]['value'],
                        "received_at": time.strftime("%Y-%m-%d %H:%M:%S")
                    }),
                    qos=1
                )
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
            print(f"{Fore.BLUE}🚀 Starting AI-Enabled MQTT Broker{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Initial Thresholds:{Style.RESET_ALL}")
            print(f"- High Power: {self.rule_manager.rules['high_power']['conditions'][0]['value']}W")
            print(f"- High Temp: {self.rule_manager.rules['high_temp']['conditions'][0]['value']}°C")
            self.client.loop_forever()
        except KeyboardInterrupt:
            print(f"{Fore.YELLOW}\n🛑 Stopping broker...{Style.RESET_ALL}")
            if self.batch_timer:
                self.batch_timer.cancel()
            if self.normal_batch:
                self.process_normal_batch()
            self.client.disconnect()
        except Exception as e:
            print(f"{Fore.RED}⛔ Broker error: {str(e)}{Style.RESET_ALL}")

if __name__ == "__main__":
    broker = AIEnergyBroker("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")
    broker.start()
