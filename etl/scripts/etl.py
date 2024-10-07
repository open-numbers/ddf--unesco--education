import pandas as pd
import zipfile
import os

ZIP_PATH = "../source/SDG.zip"
OUTPUT_DIR = "../../"
OFST_GLOBAL_PATH = "../source/ofst_global.csv"
OFST_NATIONAL_PATH = "../source/ofst_national.csv"

OFST_INDICATORS = ["ofst_1_cp", "ofst_1_m_cp", "ofst_1_f_cp"]


def extract_and_load_data():
    """
    Extracts the SDG_COUNTRY.csv, SDG_DATA_NATIONAL.csv, and SDG_LABEL.csv files from the SDG.zip archive and loads them into pandas DataFrames.
    Returns them as a tuple.
    """
    csv_files = [
        "SDG_COUNTRY.csv",
        "SDG_DATA_NATIONAL.csv",
        "SDG_DATA_REGIONAL.csv",
        "SDG_LABEL.csv",
    ]
    dfs = []

    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
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
    Makes all indicator IDs lowercase and replaces '.' with '_'.
    """
    processed_data = {}

    for indicator_id, group in df.groupby("indicator_id"):
        processed_indicator_id = indicator_id.lower().replace(".", "_")
        group = group[["country_id", "year", "value"]].copy()
        group.rename(
            columns={"country_id": "country", "value": processed_indicator_id},
            inplace=True,
        )
        group["country"] = group["country"].str.lower()
        processed_data[processed_indicator_id] = group

    return processed_data


def process_global_data(df):
    """
    We use SDG: World provided by the UNESCO regional data to get global datapoints.
    """
    global_df = df[df["region_id"] == "SDG: World"]
    global_df["region_id"] = "world"

    processed_data = {}

    for indicator_id, group in global_df.groupby("indicator_id"):
        processed_indicator_id = indicator_id.lower().replace(".", "_")
        group = group[["region_id", "year", "value"]].copy()
        group.rename(
            columns={"region_id": "global", "value": processed_indicator_id},
            inplace=True,
        )
        processed_data[processed_indicator_id] = group

    return processed_data


def process_country_id(country_df):
    """
    Processes the country data by converting column names to lowercase,
    renaming the 'country_name_en' column to 'name', and 'country_id' to 'country'.
    """
    country_df.columns = country_df.columns.str.lower()
    country_df.rename(
        columns={"country_name_en": "name", "country_id": "country"}, inplace=True
    )
    country_df["country"] = country_df["country"].str.lower()
    country_df["is--country"] = "TRUE"
    return country_df


def create_global_entity():
    data = {"global": ["world"], "name": ["World"], "is--global": ["TRUE"]}
    return pd.DataFrame(data)


def process_concept(label_df):
    """
    Processes the label data by:
    1. Converting all columns to lowercase
    2. Renaming 'indicator_id' to 'concept' and 'indicator_label_en' to 'name'
    3. Adding a 'concept_type' column with a fixed value of 'measure'
    4. Making all concept IDs lowercase and replacing '.' with '_'
    5. Stripping leading and trailing whitespaces from all cells
    """
    # Convert column names to lowercase
    label_df.columns = label_df.columns.str.lower()

    # Rename columns
    label_df.rename(
        columns={"indicator_id": "concept", "indicator_label_en": "name"}, inplace=True
    )

    # Add concept_type column
    label_df["concept_type"] = "measure"

    # Process concept IDs
    label_df["concept"] = label_df["concept"].str.lower().str.replace(".", "_")

    # Strip whitespaces from all cells
    label_df = label_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    return label_df


def create_discrete_concepts():
    """
    Creates a DataFrame for discrete concepts.
    """
    data = {
        "concept": ["name", "year", "country", "domain", "global", "geo"],
        "name": ["Name", "Year", "Country", "Domain", "World", "Geographic locations"],
        "concept_type": [
            "string",
            "time",
            "entity_set",
            "string",
            "entity_set",
            "entity_domain",
        ],
        "domain": ["", "", "geo", "", "geo", ""],
    }
    return pd.DataFrame(data)


def create_ofst_concepts():
    """
    Creates a DataFrame for OFST concepts.
    """
    data = {
        "concept": OFST_INDICATORS,
        "name": [
            "Out-of-school children of primary school age, both sexes (number)",
            "Out-of-school children of primary school age, male (number)",
            "Out-of-school children of primary school age, female (number)",
        ],
        "concept_type": ["measure"] * len(OFST_INDICATORS),
    }
    return pd.DataFrame(data)


def save_dataframe(df, filename):
    """
    Saves a DataFrame to a CSV file in the output directory.
    """
    output_path = os.path.join(OUTPUT_DIR, filename)
    df.dropna(how="any").to_csv(output_path, index=False)
    print(f"Saved: {output_path}")


def process_ofst_data(file_path, is_global=False):
    """
    Process OFST data from CSV file.
    """
    df = pd.read_csv(file_path)
    processed_data = {}

    for indicator in OFST_INDICATORS:
        # Convert indicator to uppercase, as in the source file
        source_indicator = indicator.upper()

        indicator_df = df[df["NATMON_IND"] == source_indicator].copy()
        if is_global:
            indicator_df = indicator_df[["Time", "Value"]].rename(
                columns={"Time": "year", "Value": indicator}
            )
            indicator_df["global"] = "world"
        else:
            indicator_df = indicator_df[["LOCATION", "Time", "Value"]].rename(
                columns={"LOCATION": "country", "Time": "year", "Value": indicator}
            )
            indicator_df["country"] = indicator_df["country"].str.lower()

        processed_data[indicator] = indicator_df

    return processed_data


def check_and_create_ofst_datapoints():
    """
    Check if OFST datapoints exist, and create them if they don't.
    """
    # Process national data
    national_data = process_ofst_data(OFST_NATIONAL_PATH)
    for indicator, df in national_data.items():
        filename = (
            f"national_datapoints/ddf--datapoints--{indicator}--by--country--year.csv"
        )
        full_path = os.path.join(OUTPUT_DIR, filename)
        if not os.path.exists(full_path):
            save_dataframe(df, filename)
            print(f"Created missing national datapoints for {indicator}")

    # Process global data
    global_data = process_ofst_data(OFST_GLOBAL_PATH, is_global=True)
    for indicator, df in global_data.items():
        filename = (
            f"global_datapoints/ddf--datapoints--{indicator}--by--global--year.csv"
        )
        full_path = os.path.join(OUTPUT_DIR, filename)
        if not os.path.exists(full_path):
            save_dataframe(df, filename)
            print(f"Created missing global datapoints for {indicator}")


if __name__ == "__main__":
    # create datapoints output dir if not exists
    os.makedirs(os.path.join(OUTPUT_DIR, "national_datapoints"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "global_datapoints"), exist_ok=True)

    # Extract and load data
    country_df, national_data_df, regional_data_df, label_df = extract_and_load_data()

    # Process data
    processed_national = process_national_data(national_data_df)
    processed_global = process_global_data(regional_data_df)
    processed_country = process_country_id(country_df)
    processed_concept_continuous = process_concept(label_df)
    processed_concept_discrete = create_discrete_concepts()

    # Create a set of valid indicators from the label data
    valid_indicators = set(processed_concept_continuous["concept"])

    # Save processed country data
    save_dataframe(processed_country, "ddf--entities--geo--country.csv")

    # Save global entity
    save_dataframe(create_global_entity(), "ddf--entities--geo--global.csv")

    # Process OFST concepts
    ofst_concepts = create_ofst_concepts()

    # Check if OFST indicators are already in continuous concepts
    existing_concepts = set(processed_concept_continuous["concept"])
    new_ofst_concepts = ofst_concepts[~ofst_concepts["concept"].isin(existing_concepts)]

    # Append new OFST concepts to processed_concept_continuous
    if not new_ofst_concepts.empty:
        processed_concept_continuous = pd.concat(
            [processed_concept_continuous, new_ofst_concepts], ignore_index=True
        )

    # Save processed concept data
    save_dataframe(processed_concept_continuous, "ddf--concepts--continuous.csv")
    save_dataframe(processed_concept_discrete, "ddf--concepts--discrete.csv")

    # Save processed national data (indicators)
    national_skipped_indicators = []
    national_saved_indicators = []
    for indicator_id, df in processed_national.items():
        if indicator_id in valid_indicators:
            filename = f"national_datapoints/ddf--datapoints--{indicator_id}--by--country--year.csv"
            save_dataframe(df, filename)
            national_saved_indicators.append(indicator_id)
        else:
            national_skipped_indicators.append(indicator_id)
            print(f"Skipped indicator not found in label data: {indicator_id}")

    # Save processed world data
    world_skipped_indicators = []
    world_saved_indicators = []
    for indicator_id, df in processed_global.items():
        if indicator_id in valid_indicators:
            filename = f"global_datapoints/ddf--datapoints--{indicator_id}--by--global--year.csv"
            save_dataframe(df, filename)
            world_saved_indicators.append(indicator_id)
        else:
            world_skipped_indicators.append(indicator_id)
            print(f"Skipped indicator not found in label data: {indicator_id}")

    # Print summary statistics
    print("\nSummary:")
    print(
        f"Skipped {len(national_skipped_indicators)} national indicators not found in label data."
    )
    print(
        f"Skipped {len(world_skipped_indicators)} world indicators not found in label data."
    )

    # now check ofst datapoints
    check_and_create_ofst_datapoints()

    print("\nETL process completed successfully.")
