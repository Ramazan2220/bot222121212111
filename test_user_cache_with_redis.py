#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from utils.user_cache import UserCache
from utils.redis_cache import cache_json, delete

print("🧪 test_user_cache_with_redis: start")

uc = UserCache(cache_ttl=1)  # small ttl to force refresh paths

# Clean redis key used in user_cache
delete("cache:active_users")

# First call: warm DB path
users1 = uc.get_active_users_safe()
print("first users:", users1[:5], "... size=", len(users1))

# Overwrite redis with sample users
delete("cache:active_users")
sample_users = [3000000, 3000001]

def produce_sample():
    return sample_users

_ = cache_json("cache:active_users", produce_sample, ttl_seconds=60)

# Second call: should fetch from redis
users2 = uc.get_active_users_safe()
print("second users (cached):", users2)

assert isinstance(users1, list)
assert users2 == sample_users

print("✅ test_user_cache_with_redis: OK") 