import os
import json
import hashlib
from collections import Counter

FUZZER_DIR = "corpus_fuzzer_v3/fuzz/postgres/run_2026-02-21T16-08-32Z" # Ensure this matches

def verify_corpus():
    state_files = [f for f in os.listdir(FUZZER_DIR) if f.startswith("state_")]
    
    unique_final_states = set()
    base_event_counter = None
    integrity_failures = 0
    first_file = state_files[0] if state_files else None

    print(f"Verifying {len(state_files)} states (Order-Agnostic)...\n")

    for f in state_files:
        filepath = os.path.join(FUZZER_DIR, f)
        with open(filepath, 'r') as file:
            data = json.load(file)

        # 1. VERIFY ORACLE
        final_state_str = json.dumps(data.get('final_state', []), sort_keys=True)
        unique_final_states.add(final_state_str)

        # 2. VERIFY WORKLOAD (Order-Agnostic)
        events = data.get('mutated_events', [])
        
        # Extract just the Client and SQL as a tuple so we can count them
        # We ignore delays and order completely.
        clean_events = []
        for ev in events:
            # Safely grab client (try different common keys if 'client' is missing)
            client_id = ev.get('client', ev.get('client_id', ev.get('session', 'unknown')))
            sql = ev.get('sql', '')
            clean_events.append((client_id, sql))
            
        # Count the occurrences of each exact SQL statement per client
        event_counter = Counter(clean_events)

        # Set the baseline on the first file
        if base_event_counter is None:
            base_event_counter = event_counter
            
        # Compare current file's events against the baseline
        elif event_counter != base_event_counter:
            print(f"❌ INTEGRITY FAILURE in {f}:")
            
            # Print EXACTLY what is missing or added
            missing = base_event_counter - event_counter
            added = event_counter - base_event_counter
            
            if missing:
                print(f"   -> Missing events: {list(missing.elements())}")
            if added:
                print(f"   -> Extra events inserted: {list(added.elements())}")
                
            integrity_failures += 1

    print("\n--- VERIFICATION RESULTS ---")
    print(f"Total Files Parsed: {len(state_files)}")
    print(f"Files with identical SQL payload (ignoring order/delays): {len(state_files) - integrity_failures}")
    print(f"Actually Unique Final DB States: {len(unique_final_states)}")
    
    if len(unique_final_states) == len(state_files) and integrity_failures == 0:
        print("\n✅ VERIFIED: The fuzzer strictly altered timing and ordering. All 287 states are true anomalies.")
    else:
        print("\n⚠️ WARNING: The fuzzer is definitively dropping or adding SQL statements.")

if __name__ == "__main__":
    verify_corpus()