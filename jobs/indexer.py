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

    df_ortholog = get_ortholog(spark)
    df_ortholog.show()

    df_mouse = get_mouse(spark)
    df_mouse.show()

    df_ortholog_mouse = get_ortholog_mouse(spark, df_ortholog, df_mouse)
    df_ortholog_mouse.show()


def get_ortholog(spark):
    get_table(spark, "ortholog", "o_")
    return spark.sql("SELECT * FROM ortholog")


def get_ortholog_mouse(spark, df_ortholog, df_mouse):
    get_table(spark, "ortholog", "o_")
    df_mouse.createOrReplaceTempView("mouse")

    q = '''
    SELECT o.*, m.* FROM ortholog o 
    LEFT OUTER JOIN mouse m ON m.mg_id = o.o_mouse_gene_id
    '''
    return spark.sql(q)


def get_mouse(spark):
    fusil = get_table(spark, "fusil", "f_")
    impc_adult_viability = get_table(spark, 'impc_adult_viability', 'iav_')
    impc_embryo_viability = get_table(spark, 'impc_embryo_viability', 'iev_')
    mouse_gene = get_table(spark, 'mouse_gene', 'mg_')
    mouse_gene_synonym = get_table(spark, 'mouse_gene_synonym', 'mgs_')
    mouse_gene_synonym_relation = get_table(spark, 'mouse_gene_synonym_relation', 'mgsr_')

    q = '''\
        SELECT mg.*, f.*, mgs.*, iav.*, iev.* FROM mouse_gene mg\
        LEFT OUTER JOIN impc_adult_viability iav ON iav.iav_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN impc_embryo_viability iev ON iev.iev_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN fusil f ON f.f_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN mouse_gene_synonym_relation mgsr ON mgsr.mgsr_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN mouse_gene_synonym mgs ON mgs.mgs_id = mgsr.mgsr_mouse_gene_synonym_id
    '''

    return spark.sql(q)


def get_table(spark, table_name, correlation_name):
    df = read_jdbc(spark, table_name, correlation_name)
    df = df.createOrReplaceTempView(table_name)
    return df


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
