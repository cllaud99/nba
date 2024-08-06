import pandas as pd
from minio_utils import MinioManager

data_handler = MinioManager()

objects = data_handler.list_objects_in_bucket("raw-nba")

dfs = []

if objects:
    for obj in objects:
        name = obj['Key']
        df = data_handler.read_csv_from_minio(name, 'raw-nba')
        dfs.append(df)
    big_df = pd.concat(dfs, ignore_index=True)

    temp_path = "/tmp/nba_data.csv"
    big_df.to_csv(temp_path, index=False)

    big_df = data_handler.upload_csv_to_minio(temp_path, 'silver_nba__all_seasons.csv', 'silver-nba')