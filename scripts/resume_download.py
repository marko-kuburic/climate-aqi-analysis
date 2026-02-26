"""
Resume/poll an existing CDS API job and download the result.
Uses Job 1 which is already in 'running' state.
"""
import time
import requests
import sys
import os

JOB_ID = "cefe3352-92ab-4a26-87a7-3ba5db875ed6"
API_KEY = "742d7374-46c3-400f-9072-c033552586f2"
BASE_URL = "https://cds.climate.copernicus.eu/api/retrieve/v1"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "Airflow", "files", "data", "climate", "era5_europe_monthly_1950_2024.nc")

headers = {"PRIVATE-TOKEN": API_KEY}

print(f"[INFO] Polling CDS Job: {JOB_ID}")
print(f"[INFO] Output: {OUTPUT_PATH}")
print()

while True:
    try:
        r = requests.get(f"{BASE_URL}/jobs/{JOB_ID}", headers=headers, timeout=30)
        data = r.json()
        status = data.get("status", "unknown")
        updated = data.get("updated", "")
        print(f"[{time.strftime('%H:%M:%S')}] Status: {status} (updated: {updated})")

        if status == "successful":
            print("[INFO] Job completed! Downloading result...")
            # Get download URL from results
            results_url = f"{BASE_URL}/jobs/{JOB_ID}/results"
            r2 = requests.get(results_url, headers=headers, timeout=30)
            result_data = r2.json()

            # Find the download link
            download_url = None
            if "asset" in result_data and "value" in result_data["asset"]:
                download_url = result_data["asset"]["value"]["href"]
            elif "links" in result_data:
                for link in result_data["links"]:
                    if link.get("rel") == "results":
                        download_url = link["href"]
                        break

            if not download_url:
                # Try direct download
                download_url = results_url

            print(f"[INFO] Downloading from CDS...")
            os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

            with requests.get(download_url, headers=headers, stream=True, timeout=600) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                downloaded = 0
                with open(OUTPUT_PATH, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192 * 128):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded / total * 100
                            mb = downloaded / (1024**2)
                            total_mb = total / (1024**2)
                            print(f"\r  {mb:.0f}/{total_mb:.0f} MB ({pct:.1f}%)", end="", flush=True)
                        else:
                            mb = downloaded / (1024**2)
                            print(f"\r  {mb:.0f} MB downloaded", end="", flush=True)

            file_size = os.path.getsize(OUTPUT_PATH)
            print(f"\n\n[DONE] Downloaded: {OUTPUT_PATH}")
            print(f"[DONE] Size: {file_size / (1024**3):.2f} GB")
            sys.exit(0)

        elif status == "failed":
            print(f"[ERROR] Job failed!")
            print(data)
            sys.exit(1)

        elif status in ("accepted", "running"):
            time.sleep(30)

        else:
            print(f"[WARN] Unknown status: {status}")
            time.sleep(30)

    except KeyboardInterrupt:
        print("\n[INFO] Interrupted. Job is still running on CDS server.")
        print(f"[INFO] Re-run this script to resume polling.")
        sys.exit(0)
    except Exception as e:
        print(f"[WARN] Connection error: {e}")
        print("[WARN] Retrying in 60 seconds...")
        time.sleep(60)
