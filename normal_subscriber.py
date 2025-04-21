import paho.mqtt.client as mqtt
from colorama import Fore, Style, init
import json
from datetime import datetime
import traceback

init(autoreset=True)

class NormalSubscriber:
    def __init__(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            "NormalSubscriber"
        )
        self.client.max_inflight_messages = 0
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect("localhost", 1883)

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"{Fore.GREEN}Connected to broker (RC: {rc}){Style.RESET_ALL}")
            client.subscribe("energy/normal/batch", qos=1)
            print(f"{Fore.BLUE}🌿 Subscribed to NORMAL operations (batch mode){Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Connection failed (RC: {rc}){Style.RESET_ALL}")

    def on_message(self, client, userdata, msg):
        try:
            batch_data = json.loads(msg.payload.decode())
            
            print(f"\n{Fore.BLUE}════════ BATCH PROCESSING ════════{Style.RESET_ALL}")
            print(f"{Fore.CYAN}🕒 Batch Timestamp: {batch_data.get('batch_timestamp', 'N/A')}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}📦 Batch Size: {batch_data.get('batch_size', 0)} messages{Style.RESET_ALL}")
            
            for message in batch_data.get('messages', []):
                self._process_single_message(message)
            
            print(f"{Fore.BLUE}══════════════════════════════{Style.RESET_ALL}\n")
            
        except Exception as e:
            print(f"{Fore.RED}ERROR: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()

    def _process_single_message(self, data):
        """Process an individual message from the batch"""
        try:
            timestamp = data.get('received_at', 'N/A')
            
            print(f"\n{Fore.GREEN}┌── MESSAGE @ {timestamp} ──{Style.RESET_ALL}")
            
            print(f"{Fore.YELLOW}ENERGY STATUS:{Style.RESET_ALL}")
            print(f"  Total: {data.get('energy', {}).get('total', 'N/A')}W")
            print(f"  Lights: {data.get('energy', {}).get('lights', 'N/A')}W")
            
            print(f"{Fore.CYAN}ENVIRONMENT:{Style.RESET_ALL}")
            zones = data.get('zones', {})
            for zone, values in list(zones.items())[:2]:  # Show first 2 zones
                print(f"  {zone}: {values.get('temperature', 'N/A')}°C | {values.get('humidity', 'N/A')}% RH")
            
            print(f"{Fore.MAGENTA}WEATHER:{Style.RESET_ALL}")
            print(f"  Outside: {data.get('weather', {}).get('temperature', 'N/A')}°C")
            print(f"  Humidity: {data.get('weather', {}).get('humidity', 'N/A')}%")
            
            print(f"{Fore.YELLOW}AI ANALYSIS:{Style.RESET_ALL}")
            print(f"  Priority: {data.get('ai_priority', 'N/A')}")
            print(f"  Analysis: {data.get('ai_analysis', 'N/A')}")
            
            print(f"{Fore.GREEN}└─────────────────────────────{Style.RESET_ALL}")
            
        except Exception as e:
            print(f"{Fore.RED}ERROR processing message: {str(e)}{Style.RESET_ALL}")

    def start(self):
        print(f"{Fore.BLUE}🌿 Starting NORMAL Operations Subscriber (Batch Mode){Style.RESET_ALL}")
        self.client.loop_forever()

if __name__ == "__main__":
    NormalSubscriber().start()
