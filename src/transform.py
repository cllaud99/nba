from minio_utils import read_csv_from_minio, list_objects_in_bucket

def example_read_csv_from_minio():
    object_name = "nba__1997-98_All Star_Totals.csv" 
    bucket_name = "raw-nba"

    df = read_csv_from_minio(object_name, bucket_name)
    
    if df is not None:
        print('teste')
        print(df.head())
    else:
        print("Não foi possível ler o arquivo CSV.")


if __name__ == "__main__":
    bucket_name = "raw-nba"
    objetos = list_objects_in_bucket(bucket_name)
    for objeto in objetos:
        objeto
    example_read_csv_from_minio()