#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from utils.redis_cache import cache_json, delete
import time

print("🧪 test_redis_cache_basic: start")

key = "test:cache_json_basic"

# Counter via closure
calls = {"n": 0}

def producer():
    calls["n"] += 1
    return {"value": calls["n"]}

# First call -> produce
v1 = cache_json(key, producer, ttl_seconds=60)
print("first:", v1, "calls=", calls["n"])

# Second call -> cached
v2 = cache_json(key, producer, ttl_seconds=60)
print("second (cached):", v2, "calls=", calls["n"])

# Invalidate
delete(key)

# Third call -> produce again
v3 = cache_json(key, producer, ttl_seconds=60)
print("third (after delete):", v3, "calls=", calls["n"])

assert v1 == {"value": 1}
assert v2 == {"value": 1}
assert v3 == {"value": 2}

print("✅ test_redis_cache_basic: OK") 