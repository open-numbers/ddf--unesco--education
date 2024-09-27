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

def process_data(df):
    """
    Groups the DataFrame by 'indicator_id' and processes the data.
    For each indicator, extracts 'country_id', 'year', and 'value' columns,
    and renames the 'value' column to the corresponding 'indicator_id'.
    """
    processed_data = []
    
    for indicator_id, group in df.groupby('indicator_id'):
        group = group[['country_id', 'year', 'value']]
        group.rename(columns={'value': indicator_id}, inplace=True)
        processed_data.append(group)
    
    return pd.concat(processed_data)

if __name__ == "__main__":
    df = extract_and_load_data()
    processed_df = process_data(df)
    print(processed_df.head())
