import paho.mqtt.client as mqtt
from colorama import Fore, Style, init
import json
from datetime import datetime
import traceback
import sys
import signal

init(autoreset=True)

class RedAlertSubscriber:
    def __init__(self, subscriber_id="1"):
        self.subscriber_id = subscriber_id.split('/')[-1]  # Ensure we only get the number
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            f"RedAlertSubscriber-{self.subscriber_id}"
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
            client.subscribe(f"energy/critical/subscriber{self.subscriber_id}", qos=1)
            print(f"{Fore.RED}⚡ Subscriber {self.subscriber_id} subscribed to energy/critical/subscriber{self.subscriber_id}{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Connection failed (RC: {rc}){Style.RESET_ALL}")

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
        
            print(f"\n{Fore.RED}════════ CRITICAL ALERT ════════{Style.RESET_ALL}")
            print(f"{Fore.CYAN}🕒 {data.get('received_at', 'N/A')}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}👤 Subscriber: {self.subscriber_id}{Style.RESET_ALL}")
            
            print(f"{Fore.WHITE}Trigger: {data.get('rule_trigger', 'AI detection')}")
            if 'current_threshold' in data:
                print(f"{Fore.WHITE}Threshold: {data['current_threshold']}W{Style.RESET_ALL}")
                
            timestamp = datetime.fromisoformat(data['timestamp']).strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"\n{Fore.RED}════════ CRITICAL ALERT ════════{Style.RESET_ALL}")
            print(f"{Fore.WHITE}🕒 {timestamp} | 📌 Source: {msg.topic}{Style.RESET_ALL}")
            
            print(f"\n{Fore.RED}🚨 EMERGENCY DETECTED:{Style.RESET_ALL}")
            print(f"  Total Power: {data['energy']['total']}W")
            print(f"  Lights: {data['energy']['lights']}W")
            
            print(f"\n{Fore.RED}AI ANALYSIS:{Style.RESET_ALL}")
            print(data.get('ai_analysis', 'No analysis available'))
            
            print(f"\n{Fore.RED}HOT ZONES:{Style.RESET_ALL}")
            for zone, values in list(data['zones'].items())[:3]:
                temp_color = Fore.RED if values['temperature'] > 25 else Fore.YELLOW
                print(f"  {zone}: {temp_color}{values['temperature']}°C{Style.RESET_ALL} | {values['humidity']}% RH")
            
            print(f"{Fore.RED}══════════════════════════════{Style.RESET_ALL}\n")
            
        except Exception as e:
            print(f"{Fore.RED}ERROR: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()

    def start(self):
        print(f"{Fore.RED}🚨 Starting RED ALERT Subscriber {self.subscriber_id}{Style.RESET_ALL}")
        self.client.loop_forever()

if __name__ == "__main__":
    subscriber_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    RedAlertSubscriber(subscriber_id).start()
