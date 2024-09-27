import pandas as pd
import zipfile

ZIP_PATH = "../source/SDG.zip"

def extract_and_load_data():
    """
    Extracts the SDG_COUNTRY.csv, SDG_DATA_NATIONAL.csv, and SDG_LABEL.csv files from the SDG.zip archive and loads them into pandas DataFrames.
    Returns them as a tuple.
    """
    csv_files = ["SDG_COUNTRY.csv", "SDG_DATA_NATIONAL.csv", "SDG_LABEL.csv"]
    dfs = []
    
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        for csv_file in csv_files:
            with zip_ref.open(csv_file) as file:
                df = pd.read_csv(file)
                dfs.append(df)

    return tuple(dfs)

def process_national_data(df):
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

def process_country_data(country_df):
    """
    Processes the country data by converting column names to lowercase and renaming the 'country_name_en' column to 'name'.
    """
    country_df.columns = country_df.columns.str.lower()
    country_df.rename(columns={'country_name_en': 'name'}, inplace=True)
    return country_df

if __name__ == "__main__":
    country_df, national_data_df, label_df = extract_and_load_data()
    processed_national = process_national_data(national_data_df)
    processed_country = process_country_data(country_df)
    print(list(processed_national.values())[0].head())
    print(processed_country.head())
