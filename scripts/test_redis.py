import sys
import time

def run_test(adapter):
    print("\n--- 🧪 STARTING REDIS ATOMICITY TEST ---")
    
    # 1. SETUP: Create two accounts
    # Account A has $1000 (Integer)
    # Account B has "Broken" (String) - This will cause the crash
    r = adapter.client
    r.set('acct_a', 1000)
    r.set('acct_b', "I_AM_A_STRING") 
    
    print(f"[Setup] Acct A: {r.get('acct_a')}")
    print(f"[Setup] Acct B: {r.get('acct_b')}")

    # 2. THE BAD TRANSACTION
    # We try to move $100 from A to B.
    # - Decrement A (Will Succeed)
    # - Increment B (Will FAIL because B is a string)
    print("\n[Action] Attempting Transfer of $100...")
    
    pipe = r.pipeline(transaction=True) # This sends MULTI
    pipe.decrby('acct_a', 100)          # Queued
    pipe.incrby('acct_b', 100)          # Queued
    
    try:
        results = pipe.execute()        # This sends EXEC
        print(f"[Result] Pipeline returned: {results}")
    except Exception as e:
        # Redis-py might throw an exception, or return the error in the results list
        print(f"[Result] Exception caught (Expected): {e}")

    # 3. THE VERIFICATION (Did it rollback?)
    # In a safe DB, A should still be 1000.
    # In Redis, A will be 900 (Money vanished!).
    final_a = int(r.get('acct_a'))
    
    print(f"\n[Check] Final Balance A: {final_a}")
    
    if final_a == 900:
        print("🔴 FAIL: ATOMICITY VIOLATION DETECTED!")
        print("   The first command (DECR) committed even though the second failed.")
        print("   $100 has vanished from the system.")
    elif final_a == 1000:
        print("🟢 PASS: Transaction rolled back correctly.")
    else:
        print(f"🟡 WARN: Unexpected state: {final_a}")

if __name__ == "__main__":
    from scripts.adapters import RedisAdapter
    
    adapter = RedisAdapter(host="localhost", port=6379)
    adapter.connect()
    run_test(adapter)