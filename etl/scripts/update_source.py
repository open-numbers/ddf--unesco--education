import requests
from datetime import datetime
import os

# Global variables
LAST_UPDATE = "2024-10-06"  # the last update date for current dataset.
SOURCE_URL = "https://uis.unesco.org/sites/default/files/documents/bdds/092024/SDG.zip"


def parse_date(date_string):
    """
    Parse a date string into a datetime object.
    """
    return datetime.strptime(date_string, "%Y-%m-%d")


def check_for_new_version():
    """
    Queries the UIS API endpoint and checks if there is a newer version for the education dataset.
    """
    url = "https://api.uis.unesco.org/api/public/versions/default"
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()
    education_theme = next(
        theme for theme in data["themeDataStatus"] if theme["theme"] == "EDUCATION"
    )
    api_last_update = parse_date(education_theme["lastUpdate"])
    last_update = parse_date(LAST_UPDATE)

    if api_last_update > last_update:
        print(f"New version available. Last update date from API: {api_last_update}")
        raise ValueError("New version detected")
    else:
        print("No new version available.")


def download_file():
    """
    Downloads the file from the SOURCE_URL and saves it to "../source/SDG.zip"
    """
    response = requests.get(SOURCE_URL)
    response.raise_for_status()

    # Ensure the directory exists
    os.makedirs(os.path.dirname("../source/SDG.zip"), exist_ok=True)

    with open("../source/SDG.zip", "wb") as f:
        f.write(response.content)
    print("File downloaded successfully.")


if __name__ == "__main__":
    try:
        check_for_new_version()
        # If no new version is detected, download the file
        download_file()
    except ValueError as e:
        print(str(e))
        print("Skipping file download due to new version detection.")
