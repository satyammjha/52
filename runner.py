import subprocess
import time
from datetime import datetime

def log(message):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now}] {message}")

def run_script(script_name):
    start_time = time.time()
    log(f"Running {script_name}...")

    try:
        subprocess.run(['python', script_name], check=True)
        elapsed = time.time() - start_time
        log(f"{script_name} completed successfully in {elapsed:.2f} seconds.")
        return elapsed
    except subprocess.CalledProcessError as e:
        log(f"❌ {script_name} failed with error: {e}")
        return time.time() - start_time

def main():
    while True:
        loop_start = time.time()

        time1 = run_script('low.py')
        time2 = run_script('high.py')

        total_elapsed = time.time() - loop_start
        sleep_time = max(0, 60 - total_elapsed)

        if sleep_time > 0:
            log(f"Sleeping for {sleep_time:.2f} seconds to maintain 10-minute interval.\n")
            time.sleep(sleep_time)

if __name__ == '__main__':
    main()
