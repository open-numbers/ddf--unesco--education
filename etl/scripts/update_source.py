import requests
from datetime import datetime

# Global variable to store the last update date (manually set)
LAST_UPDATE = "2023-09-04"  # Example date, replace with actual last update date


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
    api_last_update = education_theme["lastUpdate"]

    if api_last_update > LAST_UPDATE:
        print(f"New version available. Last update date from API: {api_last_update}")
        raise ValueError("New version detected")


if __name__ == "__main__":
    check_for_new_version()
