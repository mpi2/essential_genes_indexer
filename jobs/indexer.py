import sys
import json
import re

import findspark

findspark.init('/Users/mrelac/servers/pyspark/')

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json


# From https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html:
# To get started you will need to include the JDBC driver for your particular
# database on the spark classpath. For example, to connect to postgres from
# the Spark Shell you would run the following command:
# bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar

def main(argv):
    """
    Batch Query Indexer
    :param list argv: the list elements should be:
                    [1]: jdbc connection string (dbname=batchdata, port=6082)

                    [1]: database host (localhost)
                    [2]: database name (batchdata)
                    [3]: database port (6082)
                    [4]: database username (batch_admin)
                    [5]: database password (batch_admin)
    """
    jdbc_connection_str = argv[1]
    db_user = argv[2]
    db_password = argv[3]

    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }

    # spark = SparkSession.builder.getOrCreate()

    spark = SparkSession \
        .builder \
        .config("packages", "org.postgresql:postgresql:42.2.12") \
        .master("local[*]") \
        .getOrCreate()

    stats_df = spark.read.jdbc(
        jdbc_connection_str,
        # table=f'"{idg}"',
        table='idg',
        properties=properties,
        # properties='driver=org.postgresql.Driver',

        numPartitions=5,
        column="id",
        lowerBound=1,
        upperBound=20038,
    )

    print('Hello, world!');
    # stats_df = stats_df.withColumnRenamed("statpacket", "json")
    # json_df = spark.read.json(
    #     stats_df.rdd.map(
    #         lambda row: json.dumps(
    #             json.loads(row.json, object_pairs_hook=object_pairs_hook)
    #             # normalized_phenstat_fields(
    #             #     json.loads(row.json, object_pairs_hook=object_pairs_hook)
    #             # )
    #         )
    #     )
    # )
    # json_df.write.mode("overwrite").parquet(output_path)


def object_pairs_hook(lit):
    return dict(
        [
            (re.sub(r"\{|\}|\(|\)", "|", re.sub(r"\s|,|;|\n\|\t|=", "_", key)), value)
            for (key, value) in lit
        ]
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
