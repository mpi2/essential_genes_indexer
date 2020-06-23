import sys
import os

import findspark
findspark.init(os.environ.get('SPARK_HOME'))
from pyspark.sql import SparkSession
limit = 100
# limit = None

# To get pyspark to run in Intellij:
# File -> Project Structure -> Platform Settings -> Modules
#   1. Middle panel: Django should be just under the app name
#   2. Right panel:
#      A. Specify Module SDK (e.g. Python 3.7.7)
#      B. Click the '+' to add 'Jars or directories...'
#      C. Navigate to python directory within your spark installation
#        (e.g. /Users/mrelac/servers/pyspark/python)
#          -> Choose 'OPEN'
#      D. 'Choose Categories of Selected Files'
#          -> choose 'Classes'

# Spark JDBC to other databases:
#   https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

postgres_jdbc_jar = ''
properties = ''
jdbc_connection_str = ''
output_dir = '/tmp/batchdata_output'


def main(argv):
    """
    Batch Query Indexer
    :param list argv: the list elements should be:
                    [1]: jdbc connection string
                    [2]: database username
                    [3]: database password
                    [4]: postgres jar location
                    [5]: fully-qualified output directory path
    """
    spark = initialise(argv)

    df_ortholog_mouse_and_human = get_batch_data(spark)

    df_ortholog_mouse_and_human.limit(limit).write.parquet(output_dir, 'overwrite')

    df_ortholog_mouse_and_human.limit(limit).write.csv(output_dir + ".csv", 'overwrite', header=True)
#   curl "http://localhost:8983/solr/gettingstarted/update?commit=true" -H "Content-type:application/csv" --data-binary @batchdata.csv

def get_batch_data(spark):
    get_ortholog(spark)
    df_mouse = get_mouse(spark)
    get_ortholog_mouse(spark, df_mouse)
    df_human = get_human(spark)
    get_ortholog_human(spark, df_human)
    df_ortholog_mouse_and_human = get_ortholog_mouse_and_human(spark)
    return df_ortholog_mouse_and_human


def get_ortholog(spark):
    get_table(spark, "ortholog", "o_")
    return spark.sql("SELECT * FROM ortholog")


def get_ortholog_mouse(spark, df_mouse):
    df_mouse.createOrReplaceTempView("mouse")

    q = '''
    SELECT o.*, m.* FROM ortholog o 
    LEFT OUTER JOIN mouse m ON m.mg_id = o.o_mouse_gene_id
    '''
    return spark.sql(q)


def get_ortholog_human(spark, df_mouse):
    df_mouse.createOrReplaceTempView("human")

    q = '''
    SELECT o.*, h.* FROM ortholog o 
    LEFT OUTER JOIN human h ON h.hg_id = o.o_human_gene_id
    '''
    return spark.sql(q)


def get_ortholog_mouse_and_human(spark):
    q = '''
    SELECT o.*, m.*, h.* FROM ortholog o
    LEFT OUTER JOIN mouse m ON m.mg_id = o.o_mouse_gene_id
    LEFT OUTER JOIN human h ON h.hg_id = o.o_human_gene_id
    '''
    return spark.sql(q)


def get_mouse(spark):
    get_table(spark, "fusil", "f_")
    get_table(spark, 'impc_adult_viability', 'iav_')
    get_table(spark, 'impc_embryo_viability', 'iev_')
    get_table(spark, 'mouse_gene', 'mg_')
    get_table(spark, 'mouse_gene_synonym', 'mgs_')
    get_table(spark, 'mouse_gene_synonym_relation', 'mgsr_')

    q = '''\
        SELECT mg.*, f.*, mgs.*, iav.*, iev.*\
        FROM mouse_gene mg\
        LEFT OUTER JOIN impc_adult_viability iav ON iav.iav_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN impc_embryo_viability iev ON iev.iev_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN fusil f ON f.f_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN mouse_gene_synonym_relation mgsr ON mgsr.mgsr_mouse_gene_id = mg.mg_id\
        LEFT OUTER JOIN mouse_gene_synonym mgs ON mgs.mgs_id = mgsr.mgsr_mouse_gene_synonym_id
    '''
    return spark.sql(q)


def get_human(spark):
    get_table(spark, "achilles_gene_effect", "age_")
    get_table(spark, 'clingen', 'clin_')
    get_table(spark, 'gnomad_plof', 'gnp_')
    get_table(spark, 'hgnc_gene', 'hgnc_')
    get_table(spark, 'human_gene', 'hg_')
    get_table(spark, 'human_gene_synonym', 'hgs_')
    get_table(spark, 'human_gene_synonym_relation', 'hgsr_')
    get_table(spark, 'idg', 'idg')

    q = '''\
        SELECT age.*, clin.*, gnp.*, hgnc.*, hg.*, hgs.*\
        FROM human_gene hg\
        LEFT OUTER JOIN achilles_gene_effect        AS age  ON age. age_human_gene_id  = hg.  hg_id\
        LEFT OUTER JOIN clingen                     AS clin ON clin.clin_human_gene_id = hg.  hg_id\
        LEFT OUTER JOIN gnomad_plof                 AS gnp  ON gnp. gnp_human_gene_id  = hg.  hg_id\
        LEFT OUTER JOIN hgnc_gene                   AS hgnc ON hgnc.hgnc_human_gene_id = hg.  hg_id\
        LEFT OUTER JOIN human_gene_synonym_relation AS hgsr ON hgsr.hgsr_human_gene_id = hg.  hg_id\
        LEFT OUTER JOIN human_gene_synonym          AS hgs  ON hgs. hgs_id             = hgsr.hgsr_human_gene_synonym_id
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
    if len(argv) > 5:
        global output_dir
        output_dir = argv[5]
        print('Output directory: ', output_dir)

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
