import pandas as pd
import zipfile

def extract_and_load_data():
    """
    Extracts the SDG_DATA_NATIONAL.csv file from the SDG.zip archive and loads it into a pandas DataFrame.
    """
    zip_path = "../source/SDG.zip"
    csv_file = "SDG_DATA_NATIONAL.csv"
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        with zip_ref.open(csv_file) as file:
            df = pd.read_csv(file)
    
    # Print the first few rows of the DataFrame
    print(df.head())

if __name__ == "__main__":
    extract_and_load_data()
