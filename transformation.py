from pyspark.sql import SparkSession
import argparse

def data_Process(date):
    spark = SparkSession.builder.appName("People_Data_extraction").getOrCreate()

    file_path = f'gs://airflow-project-dev/project-1/data/people_{date}.csv'

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    male_fil = df.filter(df.Sex == "Male")
    female_fil = df.filter(df.Sex == "Female")

    output_path_male = f"gs://airflow-project-dev/project-1/output/male_{date}.csv"
    output_path_female = f"gs://airflow-project-dev/project-1/output/female_{date}.csv"

    male_fil.write.csv(output_path_male, mode="overwrite", header=True)
    female_fil.write.csv(output_path_female, mode="overwrite", header=True)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process data argument")
    parser.add_argument('--date', type=str, required=True, help='Pass date in yyyymmdd format')
    args = parser.parse_args()

    data_Process(args.date)