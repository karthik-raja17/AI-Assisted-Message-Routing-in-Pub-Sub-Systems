import paho.mqtt.client as mqtt
from colorama import Fore, Style, init
import json
from datetime import datetime
import traceback
import sys
import signal
import time

init(autoreset=True)

class NormalSubscriber:
    def __init__(self, subscriber_id="1"):
        self.subscriber_id = subscriber_id.split('/')[-1]
        
        # Version-compatible client initialization
        self.client = mqtt.Client(
            f"NormalSubscriber-{self.subscriber_id}",
            protocol=mqtt.MQTTv311  # Explicit protocol version
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
        if rc == 0:
            print(f"{Fore.GREEN}Connected to broker (RC: {rc}){Style.RESET_ALL}")
            client.subscribe(f"energy/+/subscriber{self.subscriber_id}", qos=1)
            print(f"{Fore.CYAN}🔌 Subscriber {self.subscriber_id} ready for normal updates{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Connection failed (RC: {rc}){Style.RESET_ALL}")

    def on_disconnect(self, client, userdata, rc):
        if not self.shutdown_requested:
            print(f"{Fore.YELLOW}Disconnected (RC: {rc}). Reconnecting...{Style.RESET_ALL}")
            time.sleep(1)
            self._connect_with_retry()

    def on_message(self, client, userdata, msg):
        try:
            
            qsize = client._msg_queue.qsize()
            if qsize > 5:
                print(f"!QUEUE OVERLOAD: {qsize} pending!")
            
            data = json.loads(msg.payload.decode())
            topic_parts = msg.topic.split('/')
            alert_level = topic_parts[1] if len(topic_parts) > 1 else "normal"
            
            print(f"\n{Fore.CYAN}════════ ENERGY UPDATE ════════{Style.RESET_ALL}")
            print(f"{Fore.WHITE}🕒 {data.get('received_at', 'N/A')}{Style.RESET_ALL}")
            print(f"{Fore.WHITE}👤 Subscriber: {self.subscriber_id}{Style.RESET_ALL}")
            
            # Handle both single messages and batches
            messages = data.get('messages', [data])
            
            for message in messages:
                # Color code based on alert level
                if alert_level == "warning":
                    color = Fore.YELLOW
                elif alert_level == "critical":
                    color = Fore.RED
                else:
                    color = Fore.CYAN
                    
                print(f"{color}⚡ Power Status:{Style.RESET_ALL}")
                print(f"  Total: {message['energy']['total']}W")
                print(f"  Lights: {message['energy']['lights']}W")
                print(f"  HVAC: {message['energy'].get('hvac', 'N/A')}W")
                print(f"  Equipment: {message['energy'].get('equipment', 'N/A')}W")
                
                print(f"\n{color}🌡️ Environment:{Style.RESET_ALL}")
                for zone, values in message['zones'].items():
                    temp_color = Fore.RED if values['temperature'] > 25 else Fore.YELLOW if values['temperature'] > 22 else Fore.CYAN
                    print(f"  {zone}: {temp_color}{values['temperature']}°C{Style.RESET_ALL} | {values['humidity']}% RH")
                
                print(f"{Fore.CYAN}══════════════════════════════{Style.RESET_ALL}\n")
            
        except Exception as e:
            print(f"SUBSCRIBER CRASH: {str(e)}")

    def start(self):
        print(f"{Fore.CYAN}🔌 Starting NORMAL Subscriber {self.subscriber_id}{Style.RESET_ALL}")
        try:
            self.client.loop_forever()
        finally:
            # Ensure proper cleanup
            if hasattr(self.client, '_sock'):
                self.client._sock_close()

if __name__ == "__main__":
    subscriber_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    try:
        NormalSubscriber(subscriber_id).start()
    except Exception as e:
        print(f"{Fore.RED}Fatal error: {str(e)}{Style.RESET_ALL}")
        sys.exit(1)
