import requests
from datetime import datetime
import os
import zipfile
import io

# Global variables
LAST_UPDATE = "2025-02-25"  # the last update date for current dataset.
VERSION_API_URL = "https://api.uis.unesco.org/api/public/versions/default"

# OFST indicator configurations
OFST_INDICATORS = {
    "OFST.1.CP": "ofst_1_cp.csv",
    "OFST.1.M.CP": "ofst_1_m_cp.csv",
    "OFST.1.F.CP": "ofst_1_f_cp.csv"
}


def parse_date(date_string):
    """
    Parse a date string into a datetime object.
    """
    return datetime.strptime(date_string, "%Y-%m-%d")


def get_api_version_info():
    """
    Fetches the API version information and returns version string and SDG URL.
    """
    response = requests.get(VERSION_API_URL)
    response.raise_for_status()

    data = response.json()
    version = data["version"]

    education_theme = next(
        theme for theme in data["themeDataStatus"] if theme["theme"] == "EDUCATION"
    )
    api_last_update = parse_date(education_theme["lastUpdate"])
    last_update = parse_date(LAST_UPDATE)

    # Format the date for SDG.zip URL (e.g., "2025-02-23" -> "022025")
    month_year = api_last_update.strftime("%m%Y")
    sdg_url = f"https://uis.unesco.org/sites/default/files/documents/bdds/{month_year}/SDG.zip"

    if api_last_update > last_update:
        print(f"New version available. Last update date from API: {api_last_update}")
    else:
        print("No new version available.")

    return version, sdg_url


def build_ofst_url(indicator, version):
    """
    Build the OFST indicator URL using the API version.
    """
    return f"https://api.uis.unesco.org/api/public/data/indicators/export?indicator={indicator}&start=1970&end=2024&indicatorMetadata=false&footnotes=false&version={version}&format=csv"


def download_sdg_file(sdg_url):
    """
    Downloads the SDG.zip file from the provided URL and saves it to "../source/SDG.zip"
    """
    response = requests.get(sdg_url)
    response.raise_for_status()

    # Ensure the directory exists
    os.makedirs("../source", exist_ok=True)

    with open("../source/SDG.zip", "wb") as f:
        f.write(response.content)
    print("SDG.zip downloaded successfully.")


def download_and_extract_ofst_indicators(version):
    """
    Downloads all OFST indicators using the provided version and extracts CSV files from the zipped responses.
    """
    # Ensure the source directory exists
    os.makedirs("../source", exist_ok=True)

    for indicator_code, filename in OFST_INDICATORS.items():
        print(f"Downloading {indicator_code}...")

        # Build the URL for this indicator
        url = build_ofst_url(indicator_code, version)

        # Download the zipped CSV file
        response = requests.get(url)
        response.raise_for_status()

        # Extract the CSV from the zip file
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # The CSV file is located at indicator-data-export_{INDICATOR_CODE}/data.csv
            csv_path = f"indicator-data-export_{indicator_code}/data.csv"

            try:
                with zip_file.open(csv_path) as csv_file:
                    csv_content = csv_file.read()

                    # Save the CSV file to the source directory
                    output_path = f"../source/{filename}"
                    with open(output_path, "wb") as f:
                        f.write(csv_content)

                    print(f"Successfully extracted and saved {filename}")

            except KeyError:
                print(f"Warning: Could not find {csv_path} in the zip file for {indicator_code}")
                # List contents of zip file for debugging
                print(f"Zip file contents: {zip_file.namelist()}")


if __name__ == "__main__":
    # Get API version info and construct URLs
    version, sdg_url = get_api_version_info()

    # Download the files
    download_sdg_file(sdg_url)
    download_and_extract_ofst_indicators(version)

    print("All files downloaded successfully.")
