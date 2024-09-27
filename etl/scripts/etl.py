import pandas as pd
import zipfile
import os

ZIP_PATH = "../source/SDG.zip"
OUTPUT_DIR = "../../"

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
    For each indicator, extracts 'country', 'year', and 'value' columns,
    and renames the 'value' column to the corresponding 'indicator_id'.
    """
    processed_data = {}
    
    for indicator_id, group in df.groupby('indicator_id'):
        group = group[['country_id', 'year', 'value']].copy()
        group.rename(columns={'country_id': 'country', 'value': indicator_id}, inplace=True)
        processed_data[indicator_id] = group
    
    return processed_data

def process_country_id(country_df):
    """
    Processes the country data by converting column names to lowercase,
    renaming the 'country_name_en' column to 'name', and 'country_id' to 'country'.
    """
    country_df.columns = country_df.columns.str.lower()
    country_df.rename(columns={'country_name_en': 'name', 'country_id': 'country'}, inplace=True)
    return country_df

def process_concept(label_df):
    """
    Processes the label data by converting all columns to lowercase,
    renaming 'indicator_id' to 'concept' and 'indicator_label_en' to 'name',
    and adding a 'concept_type' column with a fixed value of 'measure'.
    """
    label_df.columns = label_df.columns.str.lower()
    label_df.rename(columns={'indicator_id': 'concept', 'indicator_label_en': 'name'}, inplace=True)
    label_df['concept_type'] = 'measure'
    return label_df

def save_dataframe(df, filename):
    """
    Saves a DataFrame to a CSV file in the output directory.
    """
    output_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

if __name__ == "__main__":
    # Extract and load data
    country_df, national_data_df, label_df = extract_and_load_data()
    
    # Process data
    processed_national = process_national_data(national_data_df)
    processed_country = process_country_id(country_df)
    processed_concept = process_concept(label_df)
    
    # Save processed country data
    save_dataframe(processed_country, "ddf--entities--country.csv")
    
    # Save processed concept data
    save_dataframe(processed_concept, "ddf--concepts.csv")
    
    # Save processed national data (indicators)
    for indicator_id, df in processed_national.items():
        filename = f"ddf--datapoints--{indicator_id}--by--country--year.csv"
        save_dataframe(df, filename)
    
    print("ETL process completed successfully.")
