import time
import json
import csv
import matplotlib.pyplot as plt
from test_data_generator import TestDataGenerator
from performance_monitor import PerformanceMonitor
from subscriber_monitor import monitor_subscribers
from datetime import datetime

class EvaluationFramework:
    def __init__(self):
        self.test_cases = []
        self.results = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def add_test_case(self, name, description, generator_func):
        self.test_cases.append({
            "name": name,
            "description": description,
            "generator": generator_func
        })
    
    def run_test_suite(self):
        print("Starting AI-Enabled MQTT Broker Evaluation Suite")
        
        # Initialize monitoring
        monitor = PerformanceMonitor()
        generator = TestDataGenerator()
        
        # Start subscriber monitoring in background
        sub_monitor = threading.Thread(
            target=self.run_subscriber_monitor,
            args=(f"subscriber_counts_{self.timestamp}.csv",)
        )
        sub_monitor.daemon = True
        sub_monitor.start()
        
        monitor.start()
        
        # Execute each test case
        for idx, test_case in enumerate(self.test_cases, 1):
            print(f"\n=== Test Case {idx}: {test_case['name']} ===")
            print(test_case['description'])
            
            test_case['generator'](generator)
            
            # Record test case metrics
            time.sleep(15)  # Observation period
            case_metrics = monitor.get_metrics()
            self.results.append({
                "test_case": test_case['name'],
                **case_metrics
            })
            
            # Save incremental results
            self.save_results()
            self.generate_visualizations()
        
        # Final data collection
        final_metrics = monitor.stop()
        self.save_final_results(final_metrics)
        self.generate_final_visualizations()
        
        print("\nEvaluation complete. Results and visualizations saved.")
    
    def run_subscriber_monitor(self, output_file):
        """Monitor subscriber counts and save to CSV"""
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'normal', 'critical'])
            
            start_time = time.time()
            while True:
                subs = find_subscriber_processes()
                timestamp = time.time() - start_time
                writer.writerow([
                    timestamp,
                    len(subs["normal"]),
                    len(subs["critical"])
                ])
                time.sleep(5)
    
    def save_results(self):
        """Save incremental results"""
        with open(f"test_results_{self.timestamp}.json", 'w') as f:
            json.dump(self.results, f, indent=2)
        
        with open(f"test_results_{self.timestamp}.csv", 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.results[0].keys())
            writer.writeheader()
            writer.writerows(self.results)
    
    def save_final_results(self, final_metrics):
        """Save final aggregated results"""
        summary = {
            "timestamp": self.timestamp,
            "total_test_cases": len(self.test_cases),
            "total_messages": final_metrics["total_messages"],
            "total_critical": final_metrics["critical_messages"],
            "total_normal": final_metrics["normal_messages"],
            "avg_throughput": final_metrics["throughput_msg_per_sec"],
            "avg_latency": final_metrics["average_latency_sec"]
        }
        
        with open(f"summary_{self.timestamp}.json", 'w') as f:
            json.dump(summary, f, indent=2)
    
    def generate_visualizations(self):
        """Generate visualizations after each test case"""
        if len(self.results) < 1:
            return
            
        # Throughput over test cases
        plt.figure(figsize=(10, 5))
        plt.plot(
            [r['test_case'] for r in self.results],
            [r['throughput_msg_per_sec'] for r in self.results],
            marker='o'
        )
        plt.title('Throughput by Test Case')
        plt.ylabel('Messages per second')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'throughput_{self.timestamp}.png')
        plt.close()
        
        # Message distribution
        plt.figure(figsize=(8, 8))
        plt.pie(
            [self.results[-1]['normal_messages'], self.results[-1]['critical_messages']],
            labels=['Normal', 'Critical'],
            autopct='%1.1f%%'
        )
        plt.title(f"Message Distribution: {self.results[-1]['test_case']}")
        plt.savefig(f'message_dist_{self.timestamp}_{len(self.results)}.png')
        plt.close()
    
    def generate_final_visualizations(self):
        """Generate comprehensive visualizations at the end"""
        # Load subscriber data
        try:
            timestamps, normal, critical = [], [], []
            with open(f"subscriber_counts_{self.timestamp}.csv") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    timestamps.append(float(row['timestamp']))
                    normal.append(int(row['normal']))
                    critical.append(int(row['critical']))
            
            plt.figure(figsize=(12, 6))
            plt.plot(timestamps, normal, label='Normal Subscribers')
            plt.plot(timestamps, critical, label='Critical Subscribers')
            plt.xlabel('Time (seconds)')
            plt.ylabel('Number of Subscribers')
            plt.title('Subscriber Scaling Over Time')
            plt.legend()
            plt.grid(True)
            plt.savefig(f'subscriber_scaling_{self.timestamp}.png')
            plt.close()
        except Exception as e:
            print(f"Could not generate subscriber visualization: {str(e)}")

def find_subscriber_processes():
    """Find all subscriber processes (helper for subscriber monitor)"""
    subs = {"normal": [], "critical": []}
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

def main():
    evaluator = EvaluationFramework()
    
    # Define test cases
    evaluator.add_test_case(
        "Baseline Normal Load",
        "50 normal messages with typical power and temperature values",
        lambda g: g.generate_normal_data(50)
    )
    
    evaluator.add_test_case(
        "Power Surges",
        "5 high-power messages to trigger critical routing",
        lambda g: g.generate_critical_power(5)
    )
    
    evaluator.add_test_case(
        "Temperature Alerts",
        "3 high-temperature messages to trigger critical routing",
        lambda g: g.generate_critical_temp(3)
    )
    
    evaluator.add_test_case(
        "Mixed Load",
        "Combination of normal and critical messages",
        lambda g: (
            g.generate_normal_data(20),
            g.generate_critical_power(2),
            g.generate_normal_data(20),
            g.generate_critical_temp(1)
        )
    )
    
    # Run evaluation
    evaluator.run_test_suite()

if __name__ == "__main__":
    import threading
    import psutil
    main()
