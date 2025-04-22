import json
from colorama import Fore, Style
from datetime import datetime
from collections import defaultdict
import statistics

class RuleManager:
    def __init__(self):
        self.rules = {
            "default": {"priority": "normal", "action": "route_to_normal"},
            "high_power": {
                "priority": "critical",
                "conditions": [
                    {"field": "energy.total", "op": ">", "value": 300}  # Lower initial threshold
                ],
                "action": "route_to_red"
            },
            "low_power": {
                "priority": "critical",
                "conditions": [
                    {"field": "energy.total", "op": "<", "value": 100}
                ],
                "action": "route_to_red"
            },
            "high_temp": {
                "priority": "critical",
                "conditions": [
                    {"field": "zones.*.temperature", "op": ">", "value": 28}  # Lower initial temp threshold
                ],
                "action": "route_to_red"
            }
        }
        self.message_stats = defaultdict(list)
        
    def update_stats(self, message):
        try:
            # Track energy and temperature stats
            self.message_stats['energy_total'].append(message['energy']['total'])
            for zone, values in message['zones'].items():
                self.message_stats[f'temp_{zone}'].append(values['temperature'])
            
            # Keep only recent data (100 messages max)
            for key in list(self.message_stats.keys()):
                self.message_stats[key] = self.message_stats[key][-100:]
        except Exception as e:
            print(f"{Fore.YELLOW}Stats update error: {str(e)}{Style.RESET_ALL}")

    def adapt_rules(self):
        """Dynamically adjust thresholds based on patterns"""
        changes = False
        
        # Only adapt if we have sufficient data
        if len(self.message_stats['energy_total']) >= 10:
            # Calculate new power threshold (less sensitive adjustment)
            energy_data = self.message_stats['energy_total']
            avg = statistics.mean(energy_data)
            std = statistics.stdev(energy_data) if len(energy_data) > 1 else 0
            new_threshold = avg + (0.5 * std)  # More conservative adjustment
            
            # Ensure threshold doesn't drop below minimum
            new_threshold = max(new_threshold, 300)
            
            if abs(new_threshold - self.rules['high_power']['conditions'][0]['value']) > 10:  # Significant change threshold
                self.rules['high_power']['conditions'][0]['value'] = new_threshold
                print(f"{Fore.RED}⚠️ Updated high power threshold to {new_threshold:.1f}W (μ={avg:.1f}, σ={std:.1f}){Style.RESET_ALL}")
                changes = True
        
        # Adapt temperature thresholds
        for zone in [k for k in self.message_stats if k.startswith('temp_')]:
            zone_name = zone[5:]
            temp_data = self.message_stats[zone]
            
            if len(temp_data) >= 10:
                avg_temp = statistics.mean(temp_data)
                new_temp_threshold = min(avg_temp + 3, 35)  # Cap at 35°C
                
                if abs(new_temp_threshold - self.rules['high_temp']['conditions'][0]['value']) > 1:
                    self.rules['high_temp']['conditions'][0]['value'] = new_temp_threshold
                    print(f"{Fore.YELLOW}⚠️ Adjusted {zone_name} temp threshold to {new_temp_threshold:.1f}°C{Style.RESET_ALL}")
                    changes = True
        
        return changes

    def evaluate_message(self, message):
        """Evaluate message against all rules with detailed logging"""
        try:
            print(f"\n{Fore.CYAN}=== Rule Evaluation ==={Style.RESET_ALL}")
            print(f"Current Power: {message['energy']['total']}W")
            print(f"Current Thresholds: Power>{self.rules['high_power']['conditions'][0]['value']}W, Temp>{self.rules['high_temp']['conditions'][0]['value']}°C")
            
            for rule_name, rule in self.rules.items():
                if rule_name == "default":
                    continue
                    
                if self._check_conditions(rule['conditions'], message):
                    print(f"{Fore.RED}⚠️ Triggered Rule: {rule_name}{Style.RESET_ALL}")
                    return rule['priority'], rule['action']
            
            print(f"{Fore.GREEN}No critical rules triggered{Style.RESET_ALL}")
            return self.rules['default']['priority'], self.rules['default']['action']
            
        except Exception as e:
            print(f"{Fore.YELLOW}Rule evaluation failed: {str(e)}{Style.RESET_ALL}")
            return "normal", "route_to_normal"

    def _check_conditions(self, conditions, message):
        """Check if all conditions in a rule are met with wildcard support"""
        for condition in conditions:
            field_parts = condition['field'].split('.')
            current = message
            
            try:
                # Handle wildcards and nested fields
                for part in field_parts:
                    if part == '*':
                        # Check all matching sub-fields
                        if not any(self._check_wildcard(current, condition)):
                            return False
                        break
                    else:
                        current = current[part]
                else:
                    # No wildcard, normal comparison
                    if not self._compare_values(current, condition['op'], condition['value']):
                        return False
                        
            except (KeyError, TypeError):
                return False
                
        return True

    def _check_wildcard(self, data, condition):
        """Handle wildcard field matching"""
        for key, value in data.items():
            if isinstance(value, dict):  # Nested object
                try:
                    if self._compare_values(value['temperature'], condition['op'], condition['value']):
                        return True
                except KeyError:
                    continue
        return False

    def _compare_values(self, actual, op, expected):
        """Generic comparison operator"""
        if op == ">":
            return actual > expected
        elif op == "<":
            return actual < expected
        elif op == "==":
            return actual == expected
        return False

    def save_rules(self, filepath="rules.json"):
        try:
            with open(filepath, 'w') as f:
                json.dump(self.rules, f, indent=2)
            print(f"{Fore.GREEN}Rules saved to {filepath}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}Error saving rules: {str(e)}{Style.RESET_ALL}")

    def load_rules(self, filepath="rules.json"):
        try:
            with open(filepath) as f:
                self.rules.update(json.load(f))
            print(f"{Fore.GREEN}Loaded rules from {filepath}{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.YELLOW}Could not load rules: {str(e)}{Style.RESET_ALL}")
