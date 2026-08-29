import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    df = spark.table(args.source_table).withColumn("processed_ts",f.current_timestamp())
    df.write.mode("override").format("delta").saveAsTable(args.target_table)

    row_count = spark.table(args.target_table).count()
    print(f"Done: {args.source_table} -> {args.target_table}, rows={row_count}")


if __name__ == "__main__":
    main()