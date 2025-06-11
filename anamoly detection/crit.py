import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statistics
from collections import deque, defaultdict # Added defaultdict import

# --- 1. Simulated RuleManager (mimicking your existing rule_manager.py's adaptivity) ---
class SimulatedRuleManager:
    """
    A simplified class mimicking the adaptive rule logic from your rule_manager.py.
    It updates its power and temperature thresholds based on recent statistical data.
    """
    def __init__(self, history_size=100, initial_power_threshold=450, initial_temp_threshold=32):
        self.energy_total_history = deque(maxlen=history_size)
        self.zone_temp_history = defaultdict(lambda: deque(maxlen=history_size)) # For simplicity, only track zone1
        self.power_threshold = initial_power_threshold
        self.temp_threshold = initial_temp_threshold
        self.history_size = history_size

    def update_stats(self, energy_total, zone_temps):
        """Updates the internal history with new data points."""
        self.energy_total_history.append(energy_total)
        # Assuming for this demo, we mainly focus on one zone's temperature
        if 'zone1' in zone_temps:
            self.zone_temp_history['zone1'].append(zone_temps['zone1'])

        # Adapt rules if enough historical data is present
        self._adapt_rules()

    def _adapt_rules(self):
        """Dynamically adjusts thresholds based on the stored history."""
        if len(self.energy_total_history) >= self.history_size / 2:
            avg_power = statistics.mean(self.energy_total_history)
            std_dev_power = statistics.stdev(self.energy_total_history) if len(self.energy_total_history) > 1 else 0
            # Threshold adapts to average + 0.5 standard deviations, with a floor
            new_power_threshold = max(300, avg_power + (0.5 * std_dev_power))
            if abs(new_power_threshold - self.power_threshold) > 5: # Only update if significant change
                self.power_threshold = new_power_threshold

        if 'zone1' in self.zone_temp_history and len(self.zone_temp_history['zone1']) >= self.history_size / 2:
            avg_temp = statistics.mean(self.zone_temp_history['zone1'])
            # Threshold adapts to average + 3 degrees, with a ceiling
            new_temp_threshold = min(35, avg_temp + 3)
            if abs(new_temp_threshold - self.temp_threshold) > 0.5: # Only update if significant change
                self.temp_threshold = new_temp_threshold

    def evaluate_message(self, energy_total, zone_temps):
        """
        Evaluates a message based on the current adaptive rules.
        Returns "critical" or "normal".
        """
        is_critical_power = energy_total > self.power_threshold * 1.1 # 10% above adaptive
        is_critical_low_power = energy_total < 50
        is_critical_temp = any(temp > self.temp_threshold * 1.1 for temp in zone_temps.values()) # 10% above adaptive

        if is_critical_power or is_critical_low_power or is_critical_temp:
            return "critical"
        return "normal"

# --- 2. Simulated AI Model (to avoid actual API calls and define specific behavior) ---
def simulated_ai_analyze(current_energy, current_temp, rule_manager_history, current_time_hour):
    """
    Simulates the AI's decision. This function contains the logic to demonstrate:
    - AI avoiding rule-based false positives (by understanding the 'new normal').
    - AI detecting subtle anomalies based on pattern (e.g., high usage during typical low-usage hours).
    """
    # Rule-based thresholds for reference (they are already adaptive)
    current_power_threshold = rule_manager_history.power_threshold
    current_temp_threshold = rule_manager_history.temp_threshold

    # Scenario 1: AI avoids rule-based false positive
    # If power is high but within 5% of the rule's adaptive threshold, and it's a typical high-usage hour
    # We define typical high-usage hours as 8-18 (8 AM to 6 PM)
    if current_energy > current_power_threshold and \
       current_energy < current_power_threshold * 1.05 and \
       (8 <= current_time_hour < 18):
        # This is a case where the rule might flag critical because it's just over its adaptive threshold,
        # but the AI (knowing the broader pattern of high daytime usage) classifies as normal.
        return "normal", "AI: High but within normal daytime pattern."

    # Scenario 2: AI detects subtle anomaly missed by rule
    # If power is slightly elevated during typical very low usage hours (e.g., 0-6 AM)
    # and the value is still below the rule's critical threshold.
    # Define a 'subtle anomaly' as > 200W during typical off-peak hours (0-6)
    if (0 <= current_time_hour < 6) and current_energy > 200 and current_energy < current_power_threshold * 1.1:
        # Rule might say normal because it's below its main threshold (adapted for daytime),
        # but AI knows 200W is very high for 3 AM.
        return "critical", "AI: Elevated power unusual for off-peak hours (pattern break)."
        
    # Default AI behavior (if no specific scenario applies, let it follow general logic)
    if current_energy > current_power_threshold * 1.1: # Significant threshold breach
        return "critical", "AI: Power significantly above threshold."
    if current_energy < 50: # Very low power
        return "critical", "AI: Very low power, potential equipment issue."
    if current_temp > current_temp_threshold * 1.1: # Significant temp breach
        return "critical", "AI: Temperature significantly above threshold."

    return "normal", "AI: All parameters normal based on patterns."


# --- 3. Generate Synthetic Data and Simulate Decisions ---

# Generate time points (e.g., 4 days, hourly)
time_points = pd.date_range(start='2024-01-01 00:00', periods=96, freq='H') # 4 days * 24 hours

energy_data = []
zone_temps_data = [] # For simplicity, we'll use a single zone
rule_decisions = []
ai_decisions = []
rule_power_thresholds = []
rule_temp_thresholds = []
true_anomalies = [] # Ground truth for anomalies

sim_rule_manager = SimulatedRuleManager(history_size=24) # Shorter history for faster adaptation in demo

np.random.seed(42) # for reproducibility

# Simulation loop
for i, t in enumerate(time_points):
    hour = t.hour
    
    # Base diurnal pattern (lower at night, higher during day)
    base_power = 200 + 400 * (0.5 * (1 + np.sin(hour / 24 * 2 * np.pi - np.pi / 2))) 
    base_temp = 18 + 5 * (0.5 * (1 + np.sin(hour / 24 * 2 * np.pi - np.pi / 2)))

    current_power = base_power + np.random.normal(0, 20)
    current_temp = base_temp + np.random.normal(0, 1)

    # Ensure non-negative and reasonable values
    current_power = max(50, current_power)
    current_temp = max(15, current_temp)
    
    # --- Inject Specific Scenarios ---

    # Scenario 1: Rule-based False Positive Opportunity
    # Day 2 (hours 24-48), around peak time.
    # Introduce a slightly higher "new normal" average for a period, let rules adapt.
    # Then, a value that's truly normal but slightly above the *adaptive* rule threshold.
    if i >= 24 and i < 48: # During day 2
        current_power = (base_power * 1.15) + np.random.normal(0, 15) # Overall higher baseline for this day
        current_power = max(50, current_power)
        if i == 30: # At 6 AM (simulated) on Day 2 - a relatively high but normal value in the new range
            current_power = sim_rule_manager.power_threshold * 1.06 # Slightly above the *current* rule threshold
            true_anomalies.append(False) # This is NOT a true anomaly, it's a false positive trigger
        else:
            true_anomalies.append(False) # Not a true anomaly
    elif i == 50: # Day 3 (02:00 AM) - a subtle anomaly for AI
        current_power = base_power + 250 # Significant spike for this off-peak hour
        current_temp = base_temp + 3 # Also a slight temp increase
        true_anomalies.append(True) # This IS a true anomaly
    else:
        true_anomalies.append(False) # Default to not a true anomaly

    energy_data.append(current_power)
    zone_temps_data.append({'zone1': current_temp})

    # Simulate Rule Manager's update and evaluation
    sim_rule_manager.update_stats(current_power, {'zone1': current_temp})
    rule_decision = sim_rule_manager.evaluate_message(current_power, {'zone1': current_temp})
    rule_decisions.append(rule_decision)
    rule_power_thresholds.append(sim_rule_manager.power_threshold)
    rule_temp_thresholds.append(sim_rule_manager.temp_threshold)

    # Simulate AI's decision (using the mock function)
    ai_decision, _ = simulated_ai_analyze(current_power, current_temp, sim_rule_manager, hour)
    ai_decisions.append(ai_decision)

# --- 4. Visualization ---

plt.figure(figsize=(18, 9))
plt.style.use('seaborn-v0_8-darkgrid') # Use a clean, professional style

# Plot Energy Data
plt.plot(time_points, energy_data, label='Total Energy (W)', color='#4c72b0', alpha=0.7, linewidth=1.5)

# Plot Adaptive Rule Threshold
plt.plot(time_points, rule_power_thresholds, label='Adaptive Rule Threshold (Power)', color='#c44e52', linestyle='--', alpha=0.8, linewidth=1.5)

# Mark True Anomalies
true_anomaly_times = [t for t, is_anomaly in zip(time_points, true_anomalies) if is_anomaly]
true_anomaly_values = [energy_data[i] for i, is_anomaly in enumerate(true_anomalies) if is_anomaly]
plt.plot(true_anomaly_times, true_anomaly_values, 'D', color='black', markersize=9, 
         label='True Anomaly (Ground Truth)', alpha=0.9, markeredgecolor='black', markerfacecolor='yellow')


# Mark Rule-Based Decisions vs. AI Decisions (Highlighting the "False Positive" and "Missed Anomaly")
for i, t in enumerate(time_points):
    current_power = energy_data[i]
    
    # Rule-Based False Positive: Rule says critical, but it's NOT a true anomaly and AI says normal
    if rule_decisions[i] == "critical" and not true_anomalies[i] and ai_decisions[i] == "normal":
        plt.plot(t, current_power, 'x', color='darkorange', markersize=12, mew=2, 
                 label='Rule False Positive' if 'Rule False Positive' not in plt.gca().get_legend_handles_labels()[1] else "", alpha=0.9)
    
    # AI Detected (Rule Missed): AI says critical, and it IS a true anomaly, but rule says normal
    if ai_decisions[i] == "critical" and true_anomalies[i] and rule_decisions[i] == "normal":
        plt.plot(t, current_power, '^', color='limegreen', markersize=12, 
                 label='AI Detected (Rule Missed)' if 'AI Detected (Rule Missed)' not in plt.gca().get_legend_handles_labels()[1] else "", alpha=0.9)


plt.title('AI (Pattern Recognition) vs. Adaptive Rule-Based Anomaly Detection')
plt.xlabel('Time')
plt.ylabel('Total Energy (W)')
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.grid(True, linestyle='--', alpha=0.6)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('anomaly_detection_plot2.png', dpi=300, bbox_inches='tight')
plt.show()

