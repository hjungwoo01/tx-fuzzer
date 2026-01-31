import sys
import edn_format

def analyze_raw_history(history_path):
    print(f"--- DIAGNOSTIC: Reading {history_path} ---")
    
    try:
        with open(history_path, 'r') as f:
            data = f.read()
            if data.strip().startswith("["):
                history = edn_format.loads(data)
            else:
                history = [edn_format.loads(line) for line in data.splitlines() if line.strip()]
    except Exception as e:
        print(f"CRITICAL: Could not parse EDN file. Error: {e}")
        return

    reads = 0
    writes = 0
    keys_read = set()
    keys_written = set()
    
    print("\n[Raw Sample (First 3 Ops)]:")
    for i, txn in enumerate(history[:3]):
        print(f"Txn {i}: {txn}")

    print("\n[Scanning Operations...]")
    for txn in history:
        ops = [txn]
        if isinstance(txn, dict) and txn.get(edn_format.Keyword("value")):
             ops = txn.get(edn_format.Keyword("value"))
        
        for op in ops:
            if isinstance(op, list) or isinstance(op, tuple):
                func = str(op[0])
                key = op[1]
                
                if ":read" in func:
                    reads += 1
                    keys_read.add(key)
                elif ":write" in func:
                    writes += 1
                    keys_written.add(key)

    print(f"\n--- RESULTS ---")
    print(f"Total Reads Found: {reads}")
    print(f"Total Writes Found: {writes}")
    print(f"Unique Keys Read: {len(keys_read)}")
    print(f"Unique Keys Written: {len(keys_written)}")
    
    intersection = keys_read.intersection(keys_written)
    print(f"Keys causing Conflicts (Read AND Written): {len(intersection)}")
    
    if len(intersection) > 0:
        sample = list(intersection)[:5]
        print(f"Sample Conflict Keys: {sample} (Type: {type(sample[0])})")
    else:
        print("FAIL: No intersection. The Reads and Writes are touching completely different keys.")
        print(f"Sample Read Key: {list(keys_read)[:1]}")
        print(f"Sample Write Key: {list(keys_written)[:1]}")

if __name__ == "__main__":
    analyze_raw_history("experiments/exploration_history.edn")
