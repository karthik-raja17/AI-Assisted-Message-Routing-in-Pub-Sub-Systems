from broker import AIEnergyBroker
import time

def test_ai_analysis():
    # Initialize with test API key
    broker = AIEnergyBroker("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")  # Use your actual key
    
    # Test cases designed to minimize API calls
    test_messages = [
        # Normal case (shouldn't call API)
        {
            "energy": {"total": 200, "lights": 30, "hvac": 70, "equipment": 100},
            "zones": {"zone1": {"temperature": 22}, "zone2": {"temperature": 21}}
        },
        # Borderline case (will call API)
        {
            "energy": {"total": 330, "lights": 40, "hvac": 120, "equipment": 170},
            "zones": {"zone1": {"temperature": 28}, "zone2": {"temperature": 26}}
        },
        # Clearly critical case (should call API)
        {
            "energy": {"total": 450, "lights": 50, "hvac": 200, "equipment": 200},
            "zones": {"zone1": {"temperature": 35}, "zone2": {"temperature": 30}}
        }
    ]
    
    print("Testing AI Analysis with 3 messages (1 API call expected)...")
    
    for i, message in enumerate(test_messages, 1):
        print(f"\nMessage {i}:")
        try:
            priority, analysis = broker.ai_analyze(message)
            print(f"Priority: {priority}")
            print(f"Analysis: {analysis}")
        except Exception as e:
            print(f"Error: {str(e)}")
        
        # Space out API calls
        if i < len(test_messages):
            time.sleep(5)  # Ensure we don't hit rate limits

if __name__ == "__main__":
    test_ai_analysis()
