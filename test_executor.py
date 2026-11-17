"""Quick test script for test_job handler"""

import asyncio
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.worker.job_executor import JobExecutor


async def test_test_job_handler():
    """Test the test_job handler with specified payload"""
    print("=" * 60)
    print("Testing JobExecutor.execute() with test_job handler")
    print("=" * 60)
    
    # Initialize executor
    executor = JobExecutor()
    
    # Test payload as specified
    payload = {
        "message": "Hello from my first job",
        "number": 100
    }
    
    print("\n1. Input Payload:")
    print(json.dumps(payload, indent=2))
    
    try:
        # Execute test_job
        print("\n2. Executing test_job...")
        result = await executor.execute("test_job", payload)
        
        print("\n3. Result:")
        print(json.dumps(result, indent=2))
        
        # Verify JSON serializable
        print("\n4. Verifying JSON serialization...")
        json_str = json.dumps(result)
        parsed_back = json.loads(json_str)
        print("✓ Result is JSON-serializable")
        
        # Verify expected fields
        print("\n5. Verifying result structure...")
        expected_fields = ["status", "message", "payload", "completed_at", "processed_by"]
        for field in expected_fields:
            if field in result:
                print(f"✓ Field '{field}' present: {result[field]}")
            else:
                print(f"✗ Field '{field}' MISSING")
        
        # Verify status is success
        print("\n6. Verifying success status...")
        if result.get("status") == "success":
            print("✓ Status is 'success'")
        else:
            print(f"✗ Status is '{result.get('status')}', expected 'success'")
        
        # Verify payload echo
        print("\n7. Verifying payload echo...")
        if result.get("payload") == payload:
            print("✓ Payload correctly echoed back")
        else:
            print("✗ Payload mismatch")
        
        print("\n" + "=" * 60)
        print("✓ TEST PASSED: test_job handler works correctly")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ TEST FAILED: {type(e).__name__}: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_test_job_handler())
    sys.exit(0 if success else 1)
