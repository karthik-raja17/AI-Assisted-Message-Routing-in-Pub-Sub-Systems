import psutil
import time
from colorama import Fore, Style

def find_subscriber_processes():
    """Find all subscriber processes"""
    subs = {
        "normal": [],
        "critical": []
    }
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['cmdline'] and 'python' in proc.info['name'].lower():
                cmd = ' '.join(proc.info['cmdline'])
                if 'normal_subscriber.py' in cmd:
                    subs["normal"].append(proc)
                elif 'red_subscriber.py' in cmd:
                    subs["critical"].append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
            
    return subs

def monitor_subscribers(interval=10, duration=300):
    """Monitor subscriber scaling over time"""
    print(f"Monitoring subscriber scaling for {duration} seconds...")
    
    start_time = time.time()
    timestamps = []
    normal_counts = []
    critical_counts = []
    
    try:
        while time.time() - start_time < duration:
            subs = find_subscriber_processes()
            timestamp = time.time() - start_time
            normal_count = len(subs["normal"])
            critical_count = len(subs["critical"])
            
            print(f"[{timestamp:.1f}s] Normal: {normal_count}, Critical: {critical_count}")
            
            timestamps.append(timestamp)
            normal_counts.append(normal_count)
            critical_counts.append(critical_count)
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        pass
        
    return timestamps, normal_counts, critical_counts

if __name__ == "__main__":
    print("Subscriber Scaling Monitor")
    timestamps, normal, critical = monitor_subscribers()
    
    print("\nFinal Counts:")
    print(f"Max Normal Subscribers: {max(normal)}")
    print(f"Max Critical Subscribers: {max(critical)}")
