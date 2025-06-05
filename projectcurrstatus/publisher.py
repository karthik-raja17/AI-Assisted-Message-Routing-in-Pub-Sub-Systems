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
            # Message validation schema
            self.required_fields = {
                'energy': ['total', 'lights', 'hvac', 'equipment'],
                'zones': [f'zone_{i}' for i in range(1, 10)]
            }

            # Define dtype and converters for efficient loading
            dtype_spec = {
                'Appliances': 'int32',
                'lights': 'int32',
                **{f'T{i}': 'float32' for i in range(1, 10)},
                **{f'RH_{i}': 'float32' for i in range(1, 10)},
                'T_out': 'float32',
                'Press_mm_hg': 'float32',
                'RH_out': 'float32',
                'Windspeed': 'float32'
            }

            # Load data with optimized parsing
            self.data = pd.read_csv(
                data_path,
                parse_dates=['date'],
                dtype=dtype_spec,
                engine='c',  # Use C engine for faster parsing
                true_values=['true', 'TRUE'],
                false_values=['false', 'FALSE']
            )

            # Post-load cleaning
            self._clean_data()
            
            print(f"{Fore.GREEN}Successfully loaded {len(self.data)} records{Style.RESET_ALL}")
            print("Sample record:")
            print(self.data.iloc[0][['date', 'Appliances', 'lights', 'T1', 'RH_1']])

            # MQTT Client setup
            self.client = mqtt.Client("EnergyPublisher", protocol=mqtt.MQTTv5)
            self.client.max_queued_messages = 100
            self.client.on_publish = self.on_publish
            self.client.on_log = self.on_log
            self.target_interval = 0.5  # Increased publish rate
            self.last_publish_time = 0

        except Exception as e:
            print(f"{Fore.RED}Initialization error: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()
            raise

    def _clean_data(self):
        """Ensure data quality before publishing"""
        # Fix any remaining scientific notation
        float_cols = [f'T{i}' for i in range(1,10)] + [f'RH_{i}' for i in range(1,10)]
        self.data[float_cols] = self.data[float_cols].apply(
            lambda x: x.round(2).astype('float32'))
        
        # Ensure positive values
        self.data['Appliances'] = self.data['Appliances'].abs()
        self.data['lights'] = self.data['lights'].abs()
        
        # Fill any remaining NAs
        self.data.fillna({
            'Appliances': 0,
            'lights': 0,
            **{f'T{i}': 20.0 for i in range(1,10)},
            **{f'RH_{i}': 50.0 for i in range(1,10)}
        }, inplace=True)
    def _validate_message(self, message):
        """Validate message structure against required fields"""
        try:
            for category, fields in self.required_fields.items():
                if category not in message:
                    raise ValueError(f"Missing {category} in message")
                for field in fields:
                    if field not in message[category]:
                        raise ValueError(f"Missing {field} in {category}")
            return True
        except Exception as e:
            print(f"{Fore.RED}Validation failed: {str(e)}{Style.RESET_ALL}")
            return False

    def publish_samples(self):
        try:
            print(f"{Fore.CYAN}Connecting to broker...{Style.RESET_ALL}")
            self.client.connect("localhost", 1883, keepalive=60)
            self.client.loop_start()
            time.sleep(1)  # Wait for connection
            
            print(f"{Fore.GREEN}Connected. Starting 2-second interval data stream...{Style.RESET_ALL}")
            
            for index, row in self.data.iterrows():
                try:
                    # Calculate precise sleep time
                    elapsed = time.time() - self.last_publish_time
                    sleep_time = max(0, 2.0 - elapsed)  # Strict 2-second interval
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    
                    # Generate message
                    message = {
                        "timestamp": row['date'].isoformat(),
                        "energy": {
                            "total": int(row['Appliances']),
                            "lights": int(row['lights']),
                            "hvac": int(row['Appliances'] * 0.4),
                            "equipment": int(row['Appliances'] * 0.6)
                        },
                        "zones": {
                            f"zone_{i}": {
                                "temperature": float(row[f'T{i}']),
                                "humidity": float(row[f'RH_{i}'])
                            } for i in range(1, 10)
                        }
                    }
                    
                    # Record precise publish time BEFORE sending
                    self.last_publish_time = time.time()
                    
                    # Publish with QoS 1
                    info = self.client.publish(
                        "building/energy",
                        payload=json.dumps(message),
                        qos=1
                    )
                    
                    # Verify publish
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        print(f"{Fore.YELLOW}Publish failed for row {index}{Style.RESET_ALL}")
                    
                    print(f"{Fore.CYAN}[{row['date'].time()}] Sent (MID: {info.mid}) "
                          f"Next in: {2.0-(time.time()-self.last_publish_time):.1f}s{Style.RESET_ALL}")
                    
                except KeyboardInterrupt:
                    print(f"{Fore.YELLOW}\nPublisher interrupted{Style.RESET_ALL}")
                    break
                except Exception as e:
                    print(f"{Fore.RED}Error processing row {index}: {str(e)}{Style.RESET_ALL}")
                    continue
                    
        except Exception as e:
            print(f"{Fore.RED}Fatal error: {str(e)}{Style.RESET_ALL}")
            traceback.print_exc()
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            print(f"{Fore.GREEN}Publisher stopped{Style.RESET_ALL}")
    def on_publish(self, client, userdata, mid):
        """Updated for older paho-mqtt version"""
        print(f"{Fore.YELLOW}Message published (MID: {mid}){Style.RESET_ALL}")

    def on_log(self, client, userdata, level, buf):
        print(f"{Fore.BLUE}MQTT Log: {buf}{Style.RESET_ALL}")

    def _calculate_sleep_time(self):
        """Calculate remaining time to maintain target interval"""
        elapsed = time.time() - self.last_publish_time
        return max(0, self.target_interval - elapsed)
        
if __name__ == "__main__":
    try:
        publisher = EnergyPublisher("/home/karthik-raja/research/energydata_complete.csv")
        publisher.publish_samples()
    except Exception as e:
        print(f"{Fore.RED}Failed to start publisher: {str(e)}{Style.RESET_ALL}")
