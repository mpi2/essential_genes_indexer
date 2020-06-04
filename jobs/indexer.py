import sys

import findspark

findspark.init('/Users/mrelac/servers/pyspark/')

from pyspark.sql import SparkSession

# From https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html:
# To get started you will need to include the JDBC driver for your particular
# database on the spark classpath. For example, to connect to postgres from
# the Spark Shell you would run the following command:
# bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar

postgres_jdbc_jar = ''
properties = ''
jdbc_connection_str = ''


def main(argv):
    """
    Batch Query Indexer
    :param list argv: the list elements should be:
                    [1]: jdbc connection string
                    [2]: database username
                    [3]: database password
                    [4]: postgres jar location
    """
    spark = initialise(argv)

    df_o = get_ortholog_test(spark)
    df_o.show()

    df_f, df_iav, df_iev, df_mg, df_mgs, df_mgsr = get_mouse_test(spark)
    df_mg.show()
    df_iav.show()
    df_iev.show()
    df_f.show()
    df_mgsr.show()
    df_mgs.show()


def get_mouse(spark):
    df_f = read_jdbc(spark, 'fusil', 'f_')
    df_iav = read_jdbc(spark, 'impc_adult_viability', 'iav_')
    df_iev = read_jdbc(spark, 'impc_embryo_viability', 'iev_')
    df_mg = read_jdbc(spark, 'mouse_gene', 'mg_')
    df_mgs = read_jdbc(spark, 'mouse_gene_synonym', 'mgs_')
    df_mgsr = read_jdbc(spark, 'mouse_gene_synonym_relation', 'mgsr_')
    return df_f, df_iav, df_iev, df_mg, df_mgs, df_mgsr


def get_mouse_test(spark):
    df_f = read_jdbc(spark, 'fusil', 'f_', num_partitions=4, column='id', lower_bound=1, upper_bound=20)
    df_iav = read_jdbc(spark, 'impc_adult_viability', 'iav_', num_partitions=4, column='id', lower_bound=1,
                       upper_bound=20)
    df_iev = read_jdbc(spark, 'impc_embryo_viability', 'iev_', num_partitions=4, column='id', lower_bound=1,
                       upper_bound=20)
    df_mg = read_jdbc(spark, 'mouse_gene', 'mg_', num_partitions=4, column='id', lower_bound=1, upper_bound=20)
    df_mgs = read_jdbc(spark, 'mouse_gene_synonym', 'mgs_', num_partitions=4, column='id', lower_bound=1,
                       upper_bound=20)
    df_mgsr = read_jdbc(spark, 'mouse_gene_synonym_relation', 'mgsr_', num_partitions=4, column='mouse_gene_id',
                        lower_bound=1, upper_bound=20)
    return df_f, df_iav, df_iev, df_mg, df_mgs, df_mgsr


def get_ortholog(spark):
    df_o = read_jdbc(spark, 'ortholog', 'o_')
    return df_o


def get_ortholog_test(spark):
    df_o = read_jdbc(spark, 'ortholog', 'o_', num_partitions=4, column='human_gene_id', lower_bound=1, upper_bound=20)

    return df_o


def initialise(argv):
    global jdbc_connection_str
    jdbc_connection_str = argv[1]
    db_user = argv[2]
    db_password = argv[3]
    global postgres_jdbc_jar
    postgres_jdbc_jar = argv[4]
    global properties
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }

    spark = get_spark_session()
    return spark


def read_jdbc(spark, table, correlation_name, num_partitions=None, column=None, lower_bound=None, upper_bound=None):
    df = spark.read.jdbc(
        jdbc_connection_str,
        table=table,
        properties=properties,
        numPartitions=num_partitions,
        column=column,
        lowerBound=lower_bound,
        upperBound=upper_bound,
    )
    df = remap_column_names(df, correlation_name)
    return df


def remap_column_names(df, correlation_name):
    new_column_names = list(map(lambda c: c.replace(c, correlation_name + c), df.columns))
    df = df.toDF(*new_column_names)
    return df


def get_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars", postgres_jdbc_jar) \
        .master("local[*]") \
        .getOrCreate()
    return spark


if __name__ == "__main__":
    sys.exit(main(sys.argv))
