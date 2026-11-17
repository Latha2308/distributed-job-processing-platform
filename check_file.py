#!/usr/bin/env python3
with open('app/core/redis_client.py', 'r') as f:
    content = f.read()
    lines = content.split('\n')
    
    print(f'Total lines: {len(lines)}')
    print(f'Has time import: {"import time" in content}')
    print(f'Has logging import: {"from app.core.logging_config import setup_logging" in content}')
    print(f'Has logger setup: {"logger = setup_logging()" in content}')
    print(f'Has push_to_delayed_queue: {"push_to_delayed_queue" in content}')
    print(f'Has transfer_ready_jobs_atomic: {"transfer_ready_jobs_atomic" in content}')
    print(f'Has delayed_queue_length: {"delayed_queue_length" in content}')
    print(f'Has get_delayed_queue_ready_count: {"get_delayed_queue_ready_count" in content}')
    print(f'Has peek_delayed_queue: {"peek_delayed_queue" in content}')
    print(f'Has verify_redis_connectivity: {"verify_redis_connectivity" in content}')
    
    # Find method definitions
    import re
    methods = re.findall(r'^\s{4}async def (\w+)', content, re.MULTILINE)
    print(f'\nAll async methods in RedisClient class:')
    for m in methods:
        print(f'  - {m}')
