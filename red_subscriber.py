import paho.mqtt.client as mqtt
from colorama import Fore, Style, init
import json
from datetime import datetime
import traceback

init(autoreset=True)

class RedAlertSubscriber:
    def __init__(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            "RedAlertSubscriber"
        )
        self.client.max_inflight_messages = 0
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect("localhost", 1883)

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"{Fore.GREEN}Connected to broker (RC: {rc}){Style.RESET_ALL}")
            client.subscribe("energy/critical", qos=1)
            print(f"{Fore.RED}⚡ Subscribed to RED ALERT channel{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Connection failed (RC: {rc}){Style.RESET_ALL}")

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
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
        print(f"{Fore.RED}🚨 Starting RED ALERT Subscriber{Style.RESET_ALL}")
        self.client.loop_forever()

if __name__ == "__main__":
    RedAlertSubscriber().start()
