#!/usr/bin/env python3
"""Check Redis state before making changes"""
import asyncio
import redis.asyncio as aioredis

async def check_redis():
    try:
        client = aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
        
        # Test connection
        await client.ping()
        print("✅ Redis connection successful")
        
        # Check main queue
        main_queue_len = await client.zcard("job_queue")
        print(f"📊 Main queue (job_queue): {main_queue_len} jobs")
        
        if main_queue_len > 0:
            jobs = await client.zrange("job_queue", 0, 2, withscores=True)
            print(f"📋 Sample jobs in main queue:")
            for job_data, score in jobs[:2]:
                print(f"   Score: {score}")
                print(f"   Data: {job_data[:200]}")
        
        # Check delayed queue
        delayed_queue_len = await client.zcard("job_queue_delayed")
        print(f"📊 Delayed queue (job_queue_delayed): {delayed_queue_len} jobs")
        
        await client.close()
        
    except Exception as e:
        print(f"❌ Redis check failed: {e}")

if __name__ == "__main__":
    asyncio.run(check_redis())
