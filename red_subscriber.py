import paho.mqtt.client as mqtt
from colorama import Fore, Style, init
import json
from datetime import datetime
import traceback
import sys
import signal
import time

init(autoreset=True)

class RedAlertSubscriber:
    def __init__(self, subscriber_id="1"):
        self.subscriber_id = subscriber_id.split('/')[-1]
        
        # Backward-compatible client initialization
        self.client = mqtt.Client(
            client_id=f"RedAlertSubscriber-{self.subscriber_id}",
            clean_session=True  # Older version alternative to clean_start
        )
        
        # Connection management
        self.connected = False
        self.shutdown_requested = False
        
        # Configure callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        
        # Connection with retry
        self._connect_with_retry()

    def _connect_with_retry(self, max_attempts=5):
        for attempt in range(max_attempts):
            try:
                self.client.connect("localhost", 1883, keepalive=60)
                self.connected = True
                return
            except Exception as e:
                print(f"{Fore.YELLOW}Connection attempt {attempt+1} failed: {e}{Style.RESET_ALL}")
                if attempt == max_attempts - 1:
                    raise
                time.sleep(min(2 ** attempt, 30))  # Exponential backoff

    def _handle_signal(self, signum, frame):
        print(f"{Fore.YELLOW}🚦 Received shutdown signal ({signum}), disconnecting...{Style.RESET_ALL}")
        self.shutdown_requested = True
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc):
        """Older version callback with 3 parameters"""
        if rc == 0:
            print(f"{Fore.GREEN}Connected to broker (RC: {rc}){Style.RESET_ALL}")
            client.subscribe(f"energy/critical/subscriber{self.subscriber_id}", qos=1)
            print(f"{Fore.RED}⚡ Subscriber {self.subscriber_id} ready for critical alerts{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Connection failed (RC: {rc}){Style.RESET_ALL}")

    def on_disconnect(self, client, userdata, rc):
        if not self.shutdown_requested:
            print(f"{Fore.YELLOW}Disconnected (RC: {rc}). Reconnecting...{Style.RESET_ALL}")
            time.sleep(1)
            self._connect_with_retry()

    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode('utf-8'))
            
            print(f"\n{Fore.RED}═════════ CRITICAL ALERT {self.subscriber_id} ═════════{Style.RESET_ALL}")
            print(f"{Fore.RED}Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
            print(f"{Fore.RED}Topic: {msg.topic}{Style.RESET_ALL}")
            print(f"{Fore.RED}Priority: {data.get('final_priority', 'N/A').upper()}{Style.RESET_ALL}")
            print(f"{Fore.RED}Expected Route: {data.get('expected_route', 'N/A').upper()}{Style.RESET_ALL}")
            print(f"{Fore.RED}Message ID: {data.get('message_id', 'N/A')}{Style.RESET_ALL}")

            print(f"\n{Fore.RED}🚨 EMERGENCY DETECTED:{Style.RESET_ALL}")
            print(f"  Total Power: {data['energy']['total']}W")
            print(f"  Lights: {data['energy']['lights']}W")
            
            print(f"\n{Fore.RED}AI ANALYSIS:{Style.RESET_ALL}")
            print(data.get('ai_analysis', 'No analysis available'))
            
            print(f"\n{Fore.RED}HOT ZONES:{Style.RESET_ALL}")
            for zone, values in list(data['zones'].items())[:3]: # Limiting to first 3 zones for brevity
                temp_color = Fore.RED if values['temperature'] > 25 else Fore.YELLOW
                print(f"  {zone}: {temp_color}{values['temperature']}°C{Style.RESET_ALL} | {values['humidity']}% RH")
            
            print(f"{Fore.RED}══════════════════════════════{Style.RESET_ALL}\n")
            
        except Exception as e:
            print(f"{Fore.RED}ERROR: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()

    def start(self):
        print(f"{Fore.RED}🚨 Starting RED ALERT Subscriber {self.subscriber_id}{Style.RESET_ALL}")
        try:
            self.client.loop_forever()
        finally:
            # Ensure proper cleanup
            if hasattr(self.client, '_sock'):
                self.client._sock_close()

if __name__ == "__main__":
    subscriber_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    subscriber = RedAlertSubscriber(subscriber_id)
    subscriber.start()
