import requests
from datetime import datetime, UTC
import os
import json


def fetch_and_save_flights(url):
    print ("Starting Ingestion......")
    try:
        response = requests.get(url)
        data = response.json()

        timestamp = datetime.now(UTC).strftime("%Y-%m-%d-%H-%M-%S")
        out_dir= f"data/raw/{timestamp}"

        os.makedirs(out_dir, exist_ok=True)

        with open(f"{out_dir}/flights.json", "w") as file:
            json.dump(data, file)
    
        print("Ingestion successful")

    except Exception as e:
        print(f"Something went very wrong: {e}")



if __name__ == "__main__":
    url = "https://opensky-network.org/api/states/all"

    fetch_and_save_flights(url)