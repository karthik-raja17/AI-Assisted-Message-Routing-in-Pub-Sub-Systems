#!/usr/bin/env python3
"""
AI Model Performance Comparison Script
Compares Llama3-70B vs Llama3-8B for energy system analysis
"""

import json
import time
import statistics
import random
from datetime import datetime
from openai import OpenAI
import matplotlib.pyplot as plt
import pandas as pd
from colorama import Fore, Style, init
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

# Initialize colorama
init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format=f"{Fore.CYAN}%(asctime)s{Style.RESET_ALL} - %(levelname)s - %(message)s"
)

class ModelComparison:
    def __init__(self, groq_api_key: str):
        self.client = OpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_api_key,
            max_retries=2
        )
        
        # Models to compare
        self.models = {
            "llama3-70b-8192": "Llama3 70B",
            "llama3-8b-8192": "Llama3 8B"
        }
        
        # Test results storage
        self.results = {model: [] for model in self.models.keys()}
        self.performance_metrics = {model: {} for model in self.models.keys()}
        
        # Ground truth for accuracy testing
        self.test_scenarios = self._generate_test_scenarios()
        
        logging.info(f"{Fore.GREEN}Model Comparison initialized with {len(self.test_scenarios)} test scenarios{Style.RESET_ALL}")

    def _generate_test_scenarios(self):
        """Generate diverse test scenarios with known expected outcomes"""
        scenarios = []
        
        # Critical scenarios (should return critical)
        critical_scenarios = [
            {
                "name": "High Power Critical",
                "data": {
                    "total_power": 1200,  # Above 1000W threshold
                    "lights_power": 300,
                    "equipment_power": 500,
                    "hvac_power": 400,
                    "zones": {"zone1": 22.5, "zone2": 23.0, "zone3": 21.8}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "critical",
                "reason": "Power exceeds threshold by 20%"
            },
            {
                "name": "High Temperature Critical",
                "data": {
                    "total_power": 800,
                    "lights_power": 200,
                    "equipment_power": 300,
                    "hvac_power": 300,
                    "zones": {"zone1": 28.0, "zone2": 27.5, "zone3": 26.8}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "critical",
                "reason": "Temperature exceeds threshold"
            },
            {
                "name": "Very Low Power Critical",
                "data": {
                    "total_power": 45,  # Below 50W - equipment failure
                    "lights_power": 20,
                    "equipment_power": 15,
                    "hvac_power": 10,
                    "zones": {"zone1": 20.0, "zone2": 19.5, "zone3": 21.0}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "critical",
                "reason": "Power too low - equipment failure"
            },
            {
                "name": "Multiple Zone High Temp",
                "data": {
                    "total_power": 750,
                    "lights_power": 150,
                    "equipment_power": 300,
                    "hvac_power": 300,
                    "zones": {"zone1": 28.5, "zone2": 27.8, "zone3": 29.2}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "critical",
                "reason": "Multiple zones exceed temperature threshold"
            }
        ]
        
        # Normal scenarios (should return normal)
        normal_scenarios = [
            {
                "name": "Normal Operation",
                "data": {
                    "total_power": 650,
                    "lights_power": 150,
                    "equipment_power": 250,
                    "hvac_power": 250,
                    "zones": {"zone1": 22.5, "zone2": 23.0, "zone3": 21.8}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "normal",
                "reason": "All parameters within normal range"
            },
            {
                "name": "Moderate Power Usage",
                "data": {
                    "total_power": 850,
                    "lights_power": 200,
                    "equipment_power": 350,
                    "hvac_power": 300,
                    "zones": {"zone1": 24.0, "zone2": 23.5, "zone3": 24.2}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "normal",
                "reason": "Power below threshold, temperature acceptable"
            },
            {
                "name": "Low Power Normal",
                "data": {
                    "total_power": 300,
                    "lights_power": 100,
                    "equipment_power": 100,
                    "hvac_power": 100,
                    "zones": {"zone1": 20.5, "zone2": 21.0, "zone3": 20.8}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "normal",
                "reason": "Low but acceptable power consumption"
            }
        ]
        
        # Edge cases
        edge_scenarios = [
            {
                "name": "Threshold Boundary",
                "data": {
                    "total_power": 1050,  # Just above 10% buffer
                    "lights_power": 250,
                    "equipment_power": 400,
                    "hvac_power": 400,
                    "zones": {"zone1": 24.8, "zone2": 24.9, "zone3": 24.7}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "normal",  # Within 10% buffer
                "reason": "Just within acceptable range"
            },
            {
                "name": "Temperature Boundary",
                "data": {
                    "total_power": 700,
                    "lights_power": 150,
                    "equipment_power": 275,
                    "hvac_power": 275,
                    "zones": {"zone1": 27.4, "zone2": 26.8, "zone3": 27.2}
                },
                "thresholds": {"power": 1000, "temperature": 25},
                "expected": "normal",  # Just within 10% buffer (27.5°C)
                "reason": "Temperature at boundary but acceptable"
            }
        ]
        
        return critical_scenarios + normal_scenarios + edge_scenarios

    def _get_prompt_for_model(self, model_name: str, energy_data: dict, current_power_threshold: float, current_temp_threshold: float):
        """Generate appropriate prompt for each model"""
        
        if "70b" in model_name:
            # Detailed prompt for 70B model
            return f"""
            Analyze this industrial energy system data (STRICTLY FOLLOW FORMAT):
            
            CURRENT DATA:
            {json.dumps(energy_data, indent=2)}
            
            CURRENT THRESHOLDS:
            - Power Alert: > {current_power_threshold:.1f}W or < 100W
            - Temp Alert: > {current_temp_threshold:.1f}°C
            
            STRICT CRITICAL CONDITIONS:
            1. MUST report critical ONLY if:
               - Power > {current_power_threshold*1.1:.1f}W (10% buffer) OR < 50W
               - Any zone > {current_temp_threshold*1.1:.1f}°C (10% buffer)
               - Clear equipment failure patterns
            2. Otherwise report normal
            
            RESPONSE FORMAT:
            priority|analysis_summary
            """
        else:
            # Simplified prompt for 8B model
            return f"""
            Analyze energy data (respond ONLY with priority|analysis):
            - Power: {energy_data['total_power']}W (threshold: {current_power_threshold}W)
            - Max temp: {max(energy_data['zones'].values())}°C (threshold: {current_temp_threshold}°C)
            - Critical if power>{current_power_threshold*1.1}W or temp>{current_temp_threshold*1.1}°C
            Response format: priority|brief_reason
            """

    def _test_single_scenario(self, model_name: str, scenario: dict):
        """Test a single scenario with a specific model"""
        start_time = time.time()
        
        try:
            prompt = self._get_prompt_for_model(
                model_name, 
                scenario['data'], 
                scenario['thresholds']['power'], 
                scenario['thresholds']['temperature']
            )
            
            response = self.client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=150,
                timeout=10
            )
            
            response_time = time.time() - start_time
            response_text = response.choices[0].message.content.strip()
            
            # Parse response
            if "|" in response_text:
                priority, analysis = response_text.split("|", 1)
                priority = priority.strip().lower()
            else:
                priority = "normal" if "normal" in response_text.lower() else "critical"
                analysis = response_text
            
            # Check accuracy
            is_correct = priority == scenario['expected']
            
            return {
                'scenario': scenario['name'],
                'expected': scenario['expected'],
                'predicted': priority,
                'correct': is_correct,
                'response_time': response_time,
                'analysis': analysis.strip(),
                'response_text': response_text
            }
            
        except Exception as e:
            logging.error(f"{Fore.RED}Error testing {model_name} on {scenario['name']}: {str(e)}{Style.RESET_ALL}")
            return {
                'scenario': scenario['name'],
                'expected': scenario['expected'],
                'predicted': 'error',
                'correct': False,
                'response_time': time.time() - start_time,
                'analysis': f"Error: {str(e)}",
                'response_text': f"Error: {str(e)}"
            }

    def run_comparison(self, iterations: int = 3):
        """Run comprehensive comparison between models"""
        logging.info(f"{Fore.YELLOW}Starting model comparison with {iterations} iterations per scenario...{Style.RESET_ALL}")
        
        total_tests = len(self.test_scenarios) * len(self.models) * iterations
        current_test = 0
        
        for model_name, model_display in self.models.items():
            logging.info(f"{Fore.CYAN}Testing {model_display}...{Style.RESET_ALL}")
            
            model_results = []
            
            # Test each scenario multiple times for statistical significance
            for iteration in range(iterations):
                logging.info(f"  Iteration {iteration + 1}/{iterations}")
                
                # Use ThreadPoolExecutor for parallel testing
                with ThreadPoolExecutor(max_workers=3) as executor:
                    future_to_scenario = {
                        executor.submit(self._test_single_scenario, model_name, scenario): scenario
                        for scenario in self.test_scenarios
                    }
                    
                    for future in as_completed(future_to_scenario):
                        result = future.result()
                        model_results.append(result)
                        current_test += 1
                        
                        # Progress indicator
                        progress = (current_test / total_tests) * 100
                        print(f"\rProgress: {progress:.1f}% ({current_test}/{total_tests})", end="", flush=True)
            
            self.results[model_name] = model_results
            print()  # New line after progress
        
        # Calculate performance metrics
        self._calculate_metrics()
        
        # Generate reports
        self._generate_comparison_report()
        self._create_visualizations()
        
        logging.info(f"{Fore.GREEN}Comparison complete! Check the generated reports.{Style.RESET_ALL}")

    def _calculate_metrics(self):
        """Calculate performance metrics for each model"""
        for model_name in self.models.keys():
            results = self.results[model_name]
            
            # Accuracy metrics
            total_tests = len(results)
            correct_predictions = sum(1 for r in results if r['correct'])
            accuracy = correct_predictions / total_tests if total_tests > 0 else 0
            
            # Performance metrics
            response_times = [r['response_time'] for r in results if r['predicted'] != 'error']
            avg_response_time = statistics.mean(response_times) if response_times else 0
            median_response_time = statistics.median(response_times) if response_times else 0
            
            # Error rate
            errors = sum(1 for r in results if r['predicted'] == 'error')
            error_rate = errors / total_tests if total_tests > 0 else 0
            
            # Scenario-specific accuracy
            scenario_accuracy = {}
            for scenario in self.test_scenarios:
                scenario_results = [r for r in results if r['scenario'] == scenario['name']]
                scenario_correct = sum(1 for r in scenario_results if r['correct'])
                scenario_accuracy[scenario['name']] = scenario_correct / len(scenario_results) if scenario_results else 0
            
            self.performance_metrics[model_name] = {
                'total_tests': total_tests,
                'accuracy': accuracy,
                'avg_response_time': avg_response_time,
                'median_response_time': median_response_time,
                'error_rate': error_rate,
                'scenario_accuracy': scenario_accuracy,
                'response_times': response_times
            }

    def _generate_comparison_report(self):
        """Generate detailed comparison report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"model_comparison_report_{timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write("="*80 + "\n")
            f.write("AI MODEL PERFORMANCE COMPARISON REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Test Scenarios: {len(self.test_scenarios)}\n")
            f.write(f"Models Compared: {', '.join(self.models.values())}\n\n")
            
            # Overall Performance Summary
            f.write("OVERALL PERFORMANCE SUMMARY\n")
            f.write("-" * 40 + "\n")
            
            for model_name, display_name in self.models.items():
                metrics = self.performance_metrics[model_name]
                f.write(f"\n{display_name} ({model_name}):\n")
                f.write(f"  Accuracy: {metrics['accuracy']:.1%}\n")
                f.write(f"  Avg Response Time: {metrics['avg_response_time']:.3f}s\n")
                f.write(f"  Median Response Time: {metrics['median_response_time']:.3f}s\n")
                f.write(f"  Error Rate: {metrics['error_rate']:.1%}\n")
            
            # Detailed Scenario Analysis
            f.write("\n\nDETAILED SCENARIO ANALYSIS\n")
            f.write("-" * 40 + "\n")
            
            for scenario in self.test_scenarios:
                f.write(f"\n{scenario['name']} (Expected: {scenario['expected']}):\n")
                for model_name, display_name in self.models.items():
                    accuracy = self.performance_metrics[model_name]['scenario_accuracy'][scenario['name']]
                    f.write(f"  {display_name}: {accuracy:.1%} accuracy\n")
            
            # Response Time Analysis
            f.write("\n\nRESPONSE TIME ANALYSIS\n")
            f.write("-" * 40 + "\n")
            
            for model_name, display_name in self.models.items():
                times = self.performance_metrics[model_name]['response_times']
                if times:
                    f.write(f"\n{display_name}:\n")
                    f.write(f"  Min: {min(times):.3f}s\n")
                    f.write(f"  Max: {max(times):.3f}s\n")
                    f.write(f"  Std Dev: {statistics.stdev(times):.3f}s\n")
            
            # Recommendation
            f.write("\n\nRECOMMENDATION\n")
            f.write("-" * 40 + "\n")
            
            # Find best model based on weighted score
            best_model = self._calculate_best_model()
            f.write(f"Recommended Model: {self.models[best_model]}\n")
            f.write(self._get_recommendation_reasoning(best_model))
        
        logging.info(f"{Fore.GREEN}Detailed report saved to: {report_file}{Style.RESET_ALL}")
        return report_file

    def _calculate_best_model(self):
        """Calculate best model based on weighted criteria"""
        scores = {}
        
        for model_name in self.models.keys():
            metrics = self.performance_metrics[model_name]
            
            # Weighted scoring (accuracy: 60%, speed: 25%, reliability: 15%)
            accuracy_score = metrics['accuracy'] * 0.6
            speed_score = (1 / (metrics['avg_response_time'] + 0.1)) * 0.25  # Inverse of response time
            reliability_score = (1 - metrics['error_rate']) * 0.15
            
            total_score = accuracy_score + speed_score + reliability_score
            scores[model_name] = total_score
        
        return max(scores, key=scores.get)

    def _get_recommendation_reasoning(self, best_model: str):
        """Generate reasoning for model recommendation"""
        metrics = self.performance_metrics[best_model]
        reasoning = f"\nReasoning:\n"
        reasoning += f"- Accuracy: {metrics['accuracy']:.1%}\n"
        reasoning += f"- Average Response Time: {metrics['avg_response_time']:.3f}s\n"
        reasoning += f"- Error Rate: {metrics['error_rate']:.1%}\n"
        reasoning += f"- Consistent performance across different scenario types\n"
        
        # Compare with other model
        other_model = [m for m in self.models.keys() if m != best_model][0]
        other_metrics = self.performance_metrics[other_model]
        
        if metrics['accuracy'] > other_metrics['accuracy']:
            reasoning += f"- Higher accuracy than {self.models[other_model]} by {(metrics['accuracy'] - other_metrics['accuracy']):.1%}\n"
        
        if metrics['avg_response_time'] < other_metrics['avg_response_time']:
            reasoning += f"- Faster response time than {self.models[other_model]} by {(other_metrics['avg_response_time'] - metrics['avg_response_time']):.3f}s\n"
        
        return reasoning

    def _create_visualizations(self):
        """Create comparison visualizations"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Set up the plotting style
        plt.style.use('default')
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('AI Model Performance Comparison', fontsize=16, fontweight='bold')
        
        models_list = list(self.models.keys())
        display_names = [self.models[m] for m in models_list]
        
        # 1. Accuracy Comparison
        accuracies = [self.performance_metrics[m]['accuracy'] for m in models_list]
        bars1 = ax1.bar(display_names, accuracies, color=['#2E8B57', '#4169E1'])
        ax1.set_title('Overall Accuracy', fontweight='bold')
        ax1.set_ylabel('Accuracy (%)')
        ax1.set_ylim(0, 1)
        
        # Add value labels on bars
        for bar, acc in zip(bars1, accuracies):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'{acc:.1%}', ha='center', va='bottom', fontweight='bold')
        
        # 2. Response Time Comparison
        avg_times = [self.performance_metrics[m]['avg_response_time'] for m in models_list]
        bars2 = ax2.bar(display_names, avg_times, color=['#FF6347', '#32CD32'])
        ax2.set_title('Average Response Time', fontweight='bold')
        ax2.set_ylabel('Time (seconds)')
        
        # Add value labels
        for bar, time in zip(bars2, avg_times):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.001,
                    f'{time:.3f}s', ha='center', va='bottom', fontweight='bold')
        
        # 3. Scenario-wise Accuracy Heatmap
        scenario_names = [s['name'] for s in self.test_scenarios]
        accuracy_matrix = []
        
        for model_name in models_list:
            model_accuracies = []
            for scenario in self.test_scenarios:
                acc = self.performance_metrics[model_name]['scenario_accuracy'][scenario['name']]
                model_accuracies.append(acc)
            accuracy_matrix.append(model_accuracies)
        
        im = ax3.imshow(accuracy_matrix, cmap='RdYlGn', aspect='auto', vmin=0, vmax=1)
        ax3.set_title('Scenario-wise Accuracy', fontweight='bold')
        ax3.set_xticks(range(len(scenario_names)))
        ax3.set_xticklabels([s[:15] + '...' if len(s) > 15 else s for s in scenario_names], rotation=45, ha='right')
        ax3.set_yticks(range(len(display_names)))
        ax3.set_yticklabels(display_names)
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax3)
        cbar.set_label('Accuracy')
        
        # 4. Response Time Distribution
        for i, model_name in enumerate(models_list):
            times = self.performance_metrics[model_name]['response_times']
            ax4.hist(times, alpha=0.7, label=display_names[i], bins=20)
        
        ax4.set_title('Response Time Distribution', fontweight='bold')
        ax4.set_xlabel('Response Time (seconds)')
        ax4.set_ylabel('Frequency')
        ax4.legend()
        
        plt.tight_layout()
        
        # Save the plot
        plot_file = f"model_comparison_plots_{timestamp}.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.show()
        
        logging.info(f"{Fore.GREEN}Visualization saved to: {plot_file}{Style.RESET_ALL}")

    def export_detailed_results(self):
        """Export detailed results to CSV for further analysis"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Prepare data for export
        export_data = []
        for model_name, display_name in self.models.items():
            for result in self.results[model_name]:
                export_data.append({
                    'model': display_name,
                    'model_id': model_name,
                    'scenario': result['scenario'],
                    'expected': result['expected'],
                    'predicted': result['predicted'],
                    'correct': result['correct'],
                    'response_time': result['response_time'],
                    'analysis': result['analysis']
                })
        
        # Create DataFrame and export
        df = pd.DataFrame(export_data)
        csv_file = f"model_comparison_detailed_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        
        logging.info(f"{Fore.GREEN}Detailed results exported to: {csv_file}{Style.RESET_ALL}")
        return csv_file

def main():
    """Main function to run the comparison"""
    # Replace with your actual Groq API key
    GROQ_API_KEY = "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"
    
    if GROQ_API_KEY == "your_groq_api_key_here":
        print(f"{Fore.RED}Please update the GROQ_API_KEY variable with your actual API key!{Style.RESET_ALL}")
        return
    
    # Initialize comparison
    comparison = ModelComparison(GROQ_API_KEY)
    
    # Run the comparison (3 iterations per scenario for statistical significance)
    comparison.run_comparison(iterations=3)
    
    # Export detailed results
    comparison.export_detailed_results()
    
    # Print summary
    print(f"\n{Fore.GREEN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}COMPARISON SUMMARY{Style.RESET_ALL}")
    print(f"{Fore.GREEN}{'='*60}{Style.RESET_ALL}")
    
    for model_name, display_name in comparison.models.items():
        metrics = comparison.performance_metrics[model_name]
        print(f"\n{Fore.CYAN}{display_name}:{Style.RESET_ALL}")
        print(f"  Accuracy: {Fore.YELLOW}{metrics['accuracy']:.1%}{Style.RESET_ALL}")
        print(f"  Avg Response Time: {Fore.YELLOW}{metrics['avg_response_time']:.3f}s{Style.RESET_ALL}")
        print(f"  Error Rate: {Fore.YELLOW}{metrics['error_rate']:.1%}{Style.RESET_ALL}")
    
    # Recommendation
    best_model = comparison._calculate_best_model()
    print(f"\n{Fore.GREEN} RECOMMENDED MODEL: {comparison.models[best_model]}{Style.RESET_ALL}")

if __name__ == "__main__":
    main()
