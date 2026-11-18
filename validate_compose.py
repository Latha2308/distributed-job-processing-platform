#!/usr/bin/env python3
"""Validate docker-compose configuration"""
import subprocess
import sys
import os

os.chdir("/Users/chandrikakolluri/Documents/Latha/distributed-job-processing-platform")

print("=" * 60)
print("Validating docker-compose.yml")
print("=" * 60)

# Test 1: List services
print("\n1. Listing services (docker compose config --services):")
print("-" * 60)
result = subprocess.run(
    ["docker", "compose", "config", "--services"],
    capture_output=True,
    text=True
)
print(result.stdout)
if result.returncode != 0:
    print(f"ERROR: {result.stderr}")
    sys.exit(1)

# Test 2: Validate configuration
print("\n2. Validating configuration (docker compose config --quiet):")
print("-" * 60)
result = subprocess.run(
    ["docker", "compose", "config", "--quiet"],
    capture_output=True,
    text=True
)
if result.returncode == 0:
    print("✓ Configuration is valid")
else:
    print(f"✗ Configuration has errors:\n{result.stderr}")
    sys.exit(1)

print("\n" + "=" * 60)
print("All validations passed!")
print("=" * 60)
