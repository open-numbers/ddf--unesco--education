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

    return df

def process_data(df):
    """
    Groups the DataFrame by 'indicator_id' and processes the data.
    For each indicator, extracts 'country_id', 'year', and 'value' columns,
    and renames the 'value' column to the corresponding 'indicator_id'.
    """
    processed_data = {}
    
    for indicator_id, group in df.groupby('indicator_id'):
        group = group[['country_id', 'year', 'value']].copy()
        group.rename(columns={'value': indicator_id}, inplace=True)
        processed_data[indicator_id] = group
    
    return processed_data

if __name__ == "__main__":
    df = extract_and_load_data()
    processed = process_data(df)
    print(list(processed.values())[0].head())
