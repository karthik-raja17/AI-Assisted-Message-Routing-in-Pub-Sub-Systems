import paho.mqtt.client as mqtt
from colorama import Fore, Style, init
import json
from datetime import datetime
import traceback
import sys
import signal

init(autoreset=True)

class NormalSubscriber:
    def __init__(self, subscriber_id="1"):
        self.subscriber_id = subscriber_id.split('/')[-1]  # Ensure we only get the number
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            f"NormalSubscriber-{self.subscriber_id}"
        )
        self.client.max_inflight_messages = 0
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
        self.client.connect("localhost", 1883)

    def _handle_signal(self, signum, frame):
        print(f"{Fore.YELLOW}🚦 Received shutdown signal ({signum}), disconnecting...{Style.RESET_ALL}")
        self.client.disconnect()
        sys.exit(0)

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"{Fore.GREEN}Connected to broker (RC: {rc}){Style.RESET_ALL}")
            # Subscribe to the correct topic pattern
            client.subscribe(f"energy/normal/subscriber{self.subscriber_id}", qos=1)
            print(f"{Fore.BLUE}🌿 Subscriber {self.subscriber_id} subscribed to energy/normal/subscriber{self.subscriber_id}{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Connection failed (RC: {rc}){Style.RESET_ALL}")

    def on_message(self, client, userdata, msg):
        try:
            batch_data = json.loads(msg.payload.decode())
            
            print(f"\n{Fore.BLUE}════════ BATCH PROCESSING ════════{Style.RESET_ALL}")
            print(f"{Fore.CYAN}🕒 Batch Timestamp: {batch_data.get('batch_timestamp', 'N/A')}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}📦 Batch Size: {batch_data.get('batch_size', 0)} messages{Style.RESET_ALL}")
            print(f"{Fore.CYAN}👤 Subscriber: {self.subscriber_id}{Style.RESET_ALL}")
            
            for message in batch_data.get('messages', []):
                self._process_single_message(message)
            
            print(f"{Fore.BLUE}══════════════════════════════{Style.RESET_ALL}\n")
            
        except Exception as e:
            print(f"{Fore.RED}ERROR: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()

    def _process_single_message(self, data):
        try:
            timestamp = data.get('received_at', 'N/A')
            
            print(f"\n{Fore.GREEN}┌── MESSAGE @ {timestamp} ──{Style.RESET_ALL}")
            
            print(f"{Fore.YELLOW}ENERGY STATUS:{Style.RESET_ALL}")
            print(f"  Total: {data.get('energy', {}).get('total', 'N/A')}W")
            print(f"  Lights: {data.get('energy', {}).get('lights', 'N/A')}W")
            
            print(f"{Fore.CYAN}ENVIRONMENT:{Style.RESET_ALL}")
            zones = data.get('zones', {})
            for zone, values in list(zones.items())[:2]:
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
        print(f"{Fore.BLUE}🌿 Starting NORMAL Subscriber {self.subscriber_id}{Style.RESET_ALL}")
        self.client.loop_forever()

if __name__ == "__main__":
    subscriber_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    NormalSubscriber(subscriber_id).start()
