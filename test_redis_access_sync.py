#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from redis_access_sync import get_redis_sync, add_user_redis, remove_user_redis, has_access_redis
import time

print("🧪 test_redis_access_sync: start")

sync = get_redis_sync()

u = 912345
user_data = {
    "is_active": True,
    "subscription_end": None,
    "plan": "pro"
}

# Ensure clean
remove_user_redis(u)
assert has_access_redis(u) is False

# Add user
ok_add = add_user_redis(u, user_data)
print("add:", ok_add)
# Give listener a moment
time.sleep(0.1)

assert has_access_redis(u) is True
print("has_access True ok")

# Remove user
ok_rm = remove_user_redis(u)
print("remove:", ok_rm)
# Give listener a moment
time.sleep(0.1)

assert has_access_redis(u) is False
print("has_access False ok")

print("stats:", sync.get_stats())
print("✅ test_redis_access_sync: OK") 