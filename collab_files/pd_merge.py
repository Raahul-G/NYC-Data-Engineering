import os
import pandas as pd

def read_parquet_files(directory):
    # Get the list of Parquet files in the directory
    parquet_files = [file for file in os.listdir(directory) if file.endswith(".parquet")]
    main_df = pd.DataFrame()

    # Iterate over each Parquet file
    for file in parquet_files:
        file_path = os.path.join(directory, file)
        print(file_path)

        df = pd.read_parquet(file_path)

        # Rename 'Airport_fee' column if it exists
        if 'Airport_fee' in df.columns:
            df.rename(columns={'Airport_fee': 'airport_fee'}, inplace=True)

        # Drop duplicates and NaN values
        # df.drop_duplicates(inplace=True)
        # df.dropna(inplace=True)

        # Concatenate the DataFrames
        main_df = pd.concat([main_df, df], ignore_index=True)
        print('Combined..')
        del df

    folder_path = '/content/drive/MyDrive/Uber/Combined_Data'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    csv_path = os.path.join(folder_path, '2020.parquet')

    # Add '.index' column to main_df
    main_df['main_id'] = main_df.index

    # Save the combined DataFrame as a Parquet file
    main_df.to_parquet(csv_path, index=False)
    print(main_df.shape[0])
    print('DataFrame saved..')

read_parquet_files('/content/drive/MyDrive/Uber/2020')
