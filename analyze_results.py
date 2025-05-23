import json
import csv
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import seaborn as sns
import sys
import os

def analyze_results(results_file):
    """Analyze test results and generate comprehensive visualizations"""
    # Verify input file exists
    if not os.path.exists(results_file):
        print(f"Error: Results file '{results_file}' not found")
        sys.exit(1)
    
    try:
        with open(results_file) as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file '{results_file}'")
        sys.exit(1)
    
    # Extract timestamp from filename (handles various filename formats)
    try:
        filename_parts = os.path.basename(results_file).split('_')
        if len(filename_parts) >= 3:
            timestamp = '_'.join(filename_parts[-2:]).split('.')[0]
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    except Exception as e:
        print(f"Warning: Could not extract timestamp from filename: {e}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create DataFrame with error handling
    try:
        df = pd.DataFrame(data)
        if df.empty:
            print("Error: No data found in results file")
            sys.exit(1)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    os.makedirs("reports", exist_ok=True)
    
    # 1. Throughput Comparison
    try:
        plt.figure(figsize=(12, 6))
        sns.barplot(x='test_case', y='throughput_msg_per_sec', data=df)
        plt.title('Message Throughput by Test Case')
        plt.ylabel('Messages per second')
        plt.xlabel('Test Case')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'reports/throughput_comparison_{timestamp}.png')
        plt.close()
    except Exception as e:
        print(f"Warning: Could not generate throughput plot: {e}")
    
    # 2. Latency Analysis
    try:
        plt.figure(figsize=(12, 6))
        sns.boxplot(
            x='test_case',
            y='average_latency_sec',
            data=df.assign(latency=df['average_latency_sec']*1000)  # Convert to ms
        )
        plt.title('Message Processing Latency by Test Case')
        plt.ylabel('Latency (milliseconds)')
        plt.xlabel('Test Case')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'reports/latency_analysis_{timestamp}.png')
        plt.close()
    except Exception as e:
        print(f"Warning: Could not generate latency plot: {e}")
    
    # 3. Message Routing Breakdown
    try:
        routing_data = df[['test_case', 'normal_messages', 'critical_messages']].melt(
            id_vars='test_case',
            var_name='message_type',
            value_name='count'
        )
        
        plt.figure(figsize=(12, 6))
        sns.barplot(
            x='test_case',
            y='count',
            hue='message_type',
            data=routing_data
        )
        plt.title('Message Routing Breakdown by Test Case')
        plt.ylabel('Message Count')
        plt.xlabel('Test Case')
        plt.xticks(rotation=45)
        plt.legend(title='Message Type')
        plt.tight_layout()
        plt.savefig(f'reports/routing_breakdown_{timestamp}.png')
        plt.close()
    except Exception as e:
        print(f"Warning: Could not generate routing breakdown plot: {e}")
    
    # 4. Subscriber Scaling Timeline - with more robust handling
    try:
        sub_file = f'subscriber_counts_{timestamp}.csv'
        if os.path.exists(sub_file) and os.path.getsize(sub_file) > 0:
            sub_data = pd.read_csv(sub_file)
            if not sub_data.empty:
                plt.figure(figsize=(14, 7))
                plt.plot(sub_data['timestamp'], sub_data['normal'], label='Normal Subscribers')
                plt.plot(sub_data['timestamp'], sub_data['critical'], label='Critical Subscribers')
                plt.xlabel('Time (seconds)')
                plt.ylabel('Number of Subscribers')
                plt.title('Subscriber Scaling Over Time')
                plt.legend()
                plt.grid(True)
                plt.tight_layout()
                plt.savefig(f'reports/subscriber_timeline_{timestamp}.png')
                plt.close()
            else:
                print("Warning: Subscriber count data file is empty")
        else:
            print("Warning: Subscriber count data not found or empty")
    except Exception as e:
        print(f"Warning: Could not generate subscriber timeline: {e}")
    
    # Generate HTML report
    generate_html_report(timestamp)

def generate_html_report(timestamp):
    """Generate an HTML report with all visualizations"""
    try:
        # Split the timestamp into date and time components
        date_part, time_part = timestamp.split('_')
        
        # Parse the date and time
        parsed_date = datetime.strptime(date_part, "%Y%m%d")
        parsed_time = datetime.strptime(time_part, "%H%M%S").time()
        
        # Combine for display
        display_datetime = datetime.combine(parsed_date, parsed_time)
        formatted_datetime = display_datetime.strftime("%B %d, %Y %H:%M:%S")
    except Exception as e:
        print(f"Warning: Could not parse timestamp: {e}")
        formatted_datetime = timestamp  # Fallback to raw timestamp
    
    # Check which plots exist
    plots = {
        'throughput': os.path.exists(f'reports/throughput_comparison_{timestamp}.png'),
        'latency': os.path.exists(f'reports/latency_analysis_{timestamp}.png'),
        'routing': os.path.exists(f'reports/routing_breakdown_{timestamp}.png'),
        'subscriber': os.path.exists(f'reports/subscriber_timeline_{timestamp}.png')
    }
    
    html = f"""
    <html>
    <head>
        <title>Test Results - {timestamp}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #2c3e50; }}
            .plot {{ margin: 20px 0; border: 1px solid #ddd; padding: 10px; }}
            img {{ max-width: 100%; height: auto; }}
            .summary {{ background: #f9f9f9; padding: 15px; border-radius: 5px; }}
            .warning {{ color: #e67e22; }}
            .error {{ color: #e74c3c; }}
        </style>
    </head>
    <body>
        <h1>AI-Enabled MQTT Broker Test Results</h1>
        <p>Test conducted on {formatted_datetime}</p>
        
        <div class="summary">
            <h2>Key Metrics</h2>
            <p>See detailed metrics in the accompanying JSON and CSV files</p>
        </div>
        
        <h2>Performance Visualizations</h2>
    """
    
    # Add available plots to HTML
    if plots['throughput']:
        html += f"""
        <div class="plot">
            <h3>Throughput Comparison</h3>
            <img src="reports/throughput_comparison_{timestamp}.png">
        </div>
        """
    else:
        html += '<div class="warning"><p>Throughput comparison plot not available</p></div>'
    
    if plots['latency']:
        html += f"""
        <div class="plot">
            <h3>Latency Analysis</h3>
            <img src="reports/latency_analysis_{timestamp}.png">
        </div>
        """
    else:
        html += '<div class="warning"><p>Latency analysis plot not available</p></div>'
    
    if plots['routing']:
        html += f"""
        <div class="plot">
            <h3>Message Routing Breakdown</h3>
            <img src="reports/routing_breakdown_{timestamp}.png">
        </div>
        """
    else:
        html += '<div class="warning"><p>Routing breakdown plot not available</p></div>'
    
    if plots['subscriber']:
        html += f"""
        <div class="plot">
            <h3>Subscriber Scaling Timeline</h3>
            <img src="reports/subscriber_timeline_{timestamp}.png">
        </div>
        """
    else:
        html += '<div class="warning"><p>Subscriber timeline plot not available</p></div>'
    
    html += """
    </body>
    </html>
    """
    
    report_file = f"reports/test_report_{timestamp}.html"
    try:
        with open(report_file, 'w') as f:
            f.write(html)
        print(f"Successfully generated report: {report_file}")
    except Exception as e:
        print(f"Error saving HTML report: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_results.py <results_json_file>")
        sys.exit(1)
    
    analyze_results(sys.argv[1])
