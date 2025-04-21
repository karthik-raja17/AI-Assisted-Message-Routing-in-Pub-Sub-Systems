import paho.mqtt.client as mqtt
from openai import OpenAI
import json
from colorama import Fore, Style
import time
from collections import deque
from threading import Timer

class AIEnergyBroker:
    def __init__(self, groq_key: str):
        self.llm_client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_key
        )
        
        # Batch processing configuration
        self.normal_batch = deque()
        self.batch_size = 5  # Number of messages to batch
        self.batch_interval = 10  # Seconds between batch sends
        self.batch_timer = None
        
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
        """Callback when the broker connects to the MQTT server"""
        if rc == 0:
            print(f"{Fore.GREEN}Broker connected to MQTT server (RC: {rc}){Style.RESET_ALL}")
            client.subscribe("building/energy", qos=1)
            print(f"{Fore.CYAN}Subscribed to 'building/energy' topic{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Broker connection failed (RC: {rc}){Style.RESET_ALL}")
            
    def ai_analyze(self, data) -> tuple:
        """Full AI-driven analysis with dynamic routing"""
        prompt = f"""
        Analyze this industrial energy system data:
        
        ENERGY:
        - Total: {data['energy']['total']}W
        - Lights: {data['energy']['lights']}W
        
        ENVIRONMENT:
        - Avg Temp: {sum(z['temperature'] for z in data['zones'].values())/9:.1f}°C
        - Avg Humidity: {sum(z['humidity'] for z in data['zones'].values())/9:.1f}%
        
        WEATHER:
        - Outside: {data['weather']['temperature']}°C
        - Windspeed: {data['weather']['windspeed']} m/s
        
        ROUTING INSTRUCTIONS:
        1. Determine priority (critical/high/normal)
        2. Identify any anomalies
        3. Generate analysis summary
        
        RESPONSE FORMAT:
        priority|analysis summary
        """
        
        try:
            response = self.llm_client.chat.completions.create(
                model="llama3-70b-8192",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
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
                return (priority.strip().lower(), analysis.strip())
            return ("normal", response.strip())
        except Exception as e:
            print(f"{Fore.YELLOW}Failed to parse AI response: {str(e)}{Style.RESET_ALL}")
            return ("normal", "AI analysis parsing failed")

    def process_normal_batch(self):
        """Process and send the accumulated normal-priority messages"""
        if not self.normal_batch:
            return
            
        print(f"{Fore.CYAN}Processing batch of {len(self.normal_batch)} normal messages{Style.RESET_ALL}")
        
        # Create aggregated batch message
        batch_data = {
            "messages": list(self.normal_batch),
            "batch_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "batch_size": len(self.normal_batch),
            "ai_priority": "normal",
            "ai_analysis": "Batch processed normal messages"
        }
        
        # Publish the batch
        self.client.publish(
            "energy/normal/batch",
            payload=json.dumps(batch_data),
            qos=1
        )
        
        # Clear the batch
        self.normal_batch.clear()
        
        # Reset the timer
        self._reset_batch_timer()

    def _reset_batch_timer(self):
        """Reset or start the batch processing timer"""
        if self.batch_timer:
            self.batch_timer.cancel()
        self.batch_timer = Timer(self.batch_interval, self.process_normal_batch)
        self.batch_timer.start()

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            
            # Get AI analysis
            priority, analysis = self.ai_analyze(data)
            
            # Handle critical messages immediately
            if "critical" in priority:
                print(f"{Fore.RED}CRITICAL condition detected!{Style.RESET_ALL}")
                client.publish(
                    "energy/critical",
                    payload=json.dumps({
                        **data,
                        "ai_priority": priority,
                        "ai_analysis": analysis
                    }),
                    qos=1
                )
            else:
                # Add normal messages to batch
                self.normal_batch.append({
                    **data,
                    "ai_priority": priority,
                    "ai_analysis": analysis,
                    "received_at": time.strftime("%Y-%m-%d %H:%M:%S")
                })
                
                print(f"{Fore.GREEN}Added to normal batch (size: {len(self.normal_batch)}){Style.RESET_ALL}")
                
                # Start batch timer if not running
                if not self.batch_timer:
                    self._reset_batch_timer()
                
                # Process batch if size threshold reached
                if len(self.normal_batch) >= self.batch_size:
                    self.process_normal_batch()
            
            print(f"{Fore.CYAN}AI Analysis:{Style.RESET_ALL}")
            print(f"Priority: {priority}")
            print(f"Analysis: {analysis}\n")
            
        except json.JSONDecodeError:
            print(f"{Fore.RED}Error: Invalid JSON payload received{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}Error processing message: {str(e)}{Style.RESET_ALL}")

    def start(self):
        try:
            print(f"{Fore.BLUE}🚀 Starting AI-Enabled MQTT Broker{Style.RESET_ALL}")
            print(f"{Fore.CYAN}Batch Configuration:{Style.RESET_ALL}")
            print(f"- Size: {self.batch_size} messages")
            print(f"- Interval: {self.batch_interval} seconds")
            self.client.loop_forever()
        except KeyboardInterrupt:
            print(f"{Fore.YELLOW}\n🛑 Stopping broker...{Style.RESET_ALL}")
            if self.batch_timer:
                self.batch_timer.cancel()
            if self.normal_batch:
                self.process_normal_batch()  # Process any remaining messages
            self.client.disconnect()
        except Exception as e:
            print(f"{Fore.RED}⛔ Broker error: {str(e)}{Style.RESET_ALL}")

if __name__ == "__main__":
    broker = AIEnergyBroker("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")
    broker.start()
