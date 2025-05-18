import paho.mqtt.client as mqtt
import pandas as pd
import json
import time
from colorama import Fore, Style
import traceback
from datetime import datetime

class EnergyPublisher:
    def __init__(self, data_path):
        try:
            self.data = pd.read_csv(data_path, parse_dates=['date'])
            print(f"{Fore.GREEN}Successfully loaded {len(self.data)} records{Style.RESET_ALL}")
            
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                "EnergyPublisher",
                clean_session=False
            )
            self.client.max_queued_messages = 0
            
            # Add callbacks for debugging
            self.client.on_publish = self.on_publish
            self.client.on_log = self.on_log
            
            # Rate limiting variables
            self.target_interval = 2  # seconds between messages
            self.last_publish_time = 0
            
        except Exception as e:
            print(f"{Fore.RED}Initialization error: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()
            raise

    def on_publish(self, client, userdata, mid, reason_code, properties):
        print(f"{Fore.YELLOW}Message published (MID: {mid}, RC: {reason_code}){Style.RESET_ALL}")

    def on_log(self, client, userdata, level, buf):
        print(f"{Fore.BLUE}MQTT Log: {buf}{Style.RESET_ALL}")

    def _calculate_sleep_time(self):
        """Calculate remaining time to maintain target interval"""
        elapsed = time.time() - self.last_publish_time
        return max(0, self.target_interval - elapsed)

    def publish_samples(self):
        try:
            print(f"{Fore.CYAN}Connecting to broker...{Style.RESET_ALL}")
            self.client.connect("localhost", 1883, keepalive=60)
            
            # Start network loop in background
            self.client.loop_start()
            time.sleep(1)  # Wait for connection
            
            print(f"{Fore.GREEN}Connected to broker. Starting data stream...{Style.RESET_ALL}")
            
            for index, row in self.data.iterrows():
                try:
                    # Maintain consistent publishing rate
                    sleep_time = self._calculate_sleep_time()
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    
                    message = {
                        "timestamp": row['date'].isoformat(),
                        "energy": {
                            "total": int(row['Appliances']),
                            "lights": int(row['lights'])
                        },
                        "zones": {
                            f"zone_{i}": {
                                "temperature": float(row[f'T{i}']),
                                "humidity": float(row[f'RH_{i}'])
                            } for i in range(1, 10)
                        },
                        "weather": {
                            "temperature": float(row['T_out']),
                            "humidity": float(row['RH_out']),
                            "pressure": float(row['Press_mm_hg']),
                            "windspeed": float(row['Windspeed'])
                        }
                    }
                    
                    # Record publish time before sending
                    self.last_publish_time = time.time()
                    
                    info = self.client.publish(
                        "building/energy",
                        payload=json.dumps(message),
                        qos=1
                    )
                    
                    # Wait for publish to complete with timeout
                    if not info.wait_for_publish(timeout=5):
                        print(f"{Fore.RED}Timeout waiting for message {index} to publish{Style.RESET_ALL}")
                        continue
                    
                    print(f"{Fore.CYAN}[{row['date'].time()}] Sent {message['energy']['total']}W (MID: {info.mid}){Style.RESET_ALL}")
                    
                except KeyboardInterrupt:
                    print(f"{Fore.YELLOW}\nPublisher interrupted by user{Style.RESET_ALL}")
                    break
                except Exception as e:
                    print(f"{Fore.RED}Error processing row {index}: {str(e)}{Style.RESET_ALL}")
                    traceback.print_exc()
                    continue
                    
        except Exception as e:
            print(f"{Fore.RED}Fatal error: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            print(f"{Fore.GREEN}Publisher stopped cleanly{Style.RESET_ALL}")

if __name__ == "__main__":
    try:
        publisher = EnergyPublisher("/home/karthik-raja/research/energydata_complete.csv")
        publisher.publish_samples()
    except Exception as e:
        print(f"{Fore.RED}Failed to start publisher: {str(e)}{Style.RESET_ALL}")
