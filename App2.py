import time
import random
import sys

def print_slow(text, delay=0.03):
    """Print text character by character"""
    for char in text:
        sys.stdout.write(char)
        sys.stdout.flush()
        time.sleep(delay)
    print()

def loading_bar(current, total, bar_length=40):
    """Create a loading bar"""
    percent = int((current / total) * 100)
    filled = int((current / total) * bar_length)
    bar = '█' * filled + '░' * (bar_length - filled)
    return f'[{bar}] {percent}%'

# Corporate-sounding tasks
tasks = [
    "Synchronizing database records",
    "Compiling production metrics",
    "Analyzing customer data patterns",
    "Processing quarterly reports",
    "Optimizing server configurations",
    "Validating data integrity",
    "Generating statistical models",
    "Aggregating financial transactions",
    "Computing algorithmic predictions",
    "Indexing document repository",
    "Calibrating performance benchmarks",
    "Parsing XML configurations",
    "Refreshing cache systems",
    "Migrating legacy datasets",
    "Encrypting sensitive records"
]

print("\n" + "="*60)
print_slow("CORPORATE DATA PROCESSING SYSTEM v3.7.2", 0.02)
print("="*60 + "\n")
time.sleep(0.5)

print_slow("Initializing secure connection...", 0.02)
time.sleep(1)
print_slow("✓ Connected to main server [192.168.1.247]", 0.02)
time.sleep(0.8)
print_slow("✓ Authentication successful\n", 0.02)
time.sleep(0.5)

# Infinite loop
while True:
    task = random.choice(tasks)
    print(f"\n{task}...")
    
    # Random total between 500-2000 for variety
    total = random.randint(500, 2000)
    
    # Progress through the loading bar
    for i in range(total + 1):
        # Randomly slow down occasionally to look more realistic
        if random.random() < 0.1:
            time.sleep(0.05)
        else:
            time.sleep(0.01)
        
        # Update every 10 iterations to avoid too much spam
        if i % 10 == 0 or i == total:
            sys.stdout.write(f'\r{loading_bar(i, total)}')
            sys.stdout.flush()
    
    print(f'\r{loading_bar(total, total)} ✓ Complete')
    
    # Random delay between tasks
    time.sleep(random.uniform(0.5, 2.0))
    
    # Occasionally show warning messages
    if random.random() < 0.15:
        print(f"⚠ Warning: Retrying connection to node {random.randint(1,12)}...")
        time.sleep(1.5)
        print("✓ Reconnected successfully")