import sys
import os

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
essentialgenes_dir = 'essentialgenes'
ortholog_dir = 'ortholog_mapping'
local = False
limit = -1

# Doesn't work. Can't find 'solr' module. Possibly mismatched
# spark/solr connector. VERY fiddly.
# def write_df(spark, df_path):
#     df = spark.read.load(df_path)
#     df.write.format('solr')\
#         .option('zkhost','localhost:9984')\
#         .option('collection', 'batchquery')\
#         .mode('overwrite')\
#         .save()
#     print('done')

# Sample run configuration to run on ves-ebi-d9 from localhost laptop:
#
#
# [1]: jdbc connection string        jdbc:postgresql://hostname:portnum/dbname
# [2]: database username             xxxxxx
# [3]: database password             yyyyyy (ignored when running on cluster)
# [4]: output directory path         parquet
# [5]: local                         (only needed for local) if running local, any non-blank value
# [6]: limit                         (-1 = no limit)
# [7]: postgres jar location         (only needed for local) /Users/zzzzzz/Downloads/postgresql-42.2.12.jar


def main(args):
    """
    Batch Query Indexer
    :param args:
        args[1] - jdbc connection string
        args[2] - database username
        args[3] - database password
        args[4] - output directory path (optional)
        args[5] - local - set to any value if running local. Omit if running on the cluster
        args[6] - limit (ignored if running on hadoop. May be omitted.)
        args[7] - postgres jar location

    """
    spark = initialise(args)
    df_ortholog_mouse_and_human, df_ortholog_mouse_mapping_and_human_mapping = get_batch_data(spark)
    if limit >= 0:
        print('Using limit ', limit)
        df_ortholog_mouse_and_human.limit(limit).write.parquet(essentialgenes_dir, 'overwrite')
        df_ortholog_mouse_mapping_and_human_mapping.limit(limit).write.parquet(ortholog_dir, 'overwrite')
    else:
        print('No limit')
        df_ortholog_mouse_and_human.write.parquet(essentialgenes_dir, 'overwrite')
        df_ortholog_mouse_mapping_and_human_mapping.write.parquet(ortholog_dir, 'overwrite')

#   curl "http://localhost:8983/solr/gettingstarted/update?commit=true" -H "Content-type:application/csv" --data-binary @batchdata.csv

def get_batch_data(spark):
    get_ortholog(spark)
    df_mouse = get_mouse(spark)
    get_ortholog_mouse(spark, df_mouse)
    df_human = get_human(spark)
    get_ortholog_human(spark, df_human)
    df_ortholog_mouse_and_human = get_ortholog_mouse_and_human(spark)

    df_mouse_mapping = get_mouse_mapping(spark)
    get_ortholog_mouse_mapping(spark, df_mouse_mapping)
    df_human_mapping = get_human_mapping(spark)
    get_ortholog_human_mapping(spark, df_human_mapping)
    df_ortholog_mouse_mapping_and_human_mapping = get_ortholog_mouse_mapping_and_human_mapping(spark)
    return df_ortholog_mouse_and_human, df_ortholog_mouse_mapping_and_human_mapping


def get_ortholog(spark):
    get_table(spark, "ortholog", "o_", "id")
    q = '''
    SELECT o.*
    FROM ortholog o
    '''
    return spark.sql(q)


def get_ortholog_mouse(spark, df_mouse):
    df_mouse.createOrReplaceTempView("mouse")
    q = '''
    SELECT o.*, m.* FROM ortholog o 
    LEFT OUTER JOIN mouse m ON m.mg_id = o.o_mouse_gene_id
    '''
    return spark.sql(q)


def get_ortholog_human(spark, df_human):
    df_human.createOrReplaceTempView("human")
    q = '''
    SELECT o.*, h.* FROM ortholog o 
    LEFT OUTER JOIN human h ON h.hg_id = o.o_human_gene_id
    '''
    return spark.sql(q)


def get_ortholog_mouse_and_human(spark):
    get_table(spark, 'fusil', 'f_', 'id')
    q = '''
    SELECT o.*, f.*, m.*, h.* FROM ortholog o
    FULL OUTER JOIN fusil f ON f.f_ortholog_id = o.o_id
    FULL OUTER JOIN mouse m ON m.mg_id = o.o_mouse_gene_id
    FULL OUTER JOIN human h ON h.hg_id = o.o_human_gene_id
    '''
    return spark.sql(q)


def get_mouse(spark):
    get_table(spark, 'combined_adult_viability', 'cav_', 'id')
    get_table(spark, 'mouse_gene', 'mg_', 'id')
    get_table(spark, 'mouse_gene_synonym', 'mgs_', 'id')
    get_table(spark, 'mouse_gene_synonym_relation', 'mgsr_', 'mouse_gene_id')

    q = '''
        SELECT DISTINCT mg.*,
          (SELECT collect_set(mgs.mgs_synonym)
           FROM mouse_gene_synonym mgs
           JOIN mouse_gene_synonym_relation mgsr ON mgsr.mgsr_mouse_gene_synonym_id = mgs.mgs_id
           WHERE mg.mg_id = mgsr.mgsr_mouse_gene_id
           ) AS mgs_synonyms,
          cav.cav_allele_accession_id,
          cav.cav_allele_symbol,
          (SELECT collect_set(cav2.cav_category)
           FROM combined_adult_viability cav2
           WHERE cav2.cav_mouse_gene_id = mg.mg_id
          ) AS cav_category
        FROM mouse_gene mg
        LEFT OUTER JOIN combined_adult_viability cav ON cav.cav_mouse_gene_id = mg.mg_id
    '''
    return spark.sql(q)


def get_human(spark):
    get_table(spark, "achilles_gene_effect", "age_", 'id')
    get_table(spark, 'clingen', 'clin_', 'id')
    get_table(spark, 'gnomad_plof', 'gnp_', 'id')
    get_table(spark, 'hgnc_gene', 'hgnc_', 'id')
    get_table(spark, 'human_gene', 'hg_', 'id')
    get_table(spark, 'human_gene_synonym', 'hgs_', 'id')
    get_table(spark, 'human_gene_synonym_relation', 'hgsr_', 'human_gene_id')
    get_table(spark, 'idg', 'idg_', 'id')

    q = '''
        SELECT age.*, clin.*, gnp.*, hgnc.*, hg.*,
          (SELECT collect_set(hgs.hgs_synonym)
           FROM human_gene_synonym hgs
           JOIN human_gene_synonym_relation hgsr ON hgsr.hgsr_human_gene_synonym_id = hgs.hgs_id
           WHERE hg.hg_id = hgsr.hgsr_human_gene_id
           ) AS hgs_synonyms,
        idg.*
        FROM human_gene hg
        LEFT OUTER JOIN achilles_gene_effect        AS age  ON age. age_human_gene_id  = hg.  hg_id
        LEFT OUTER JOIN clingen                     AS clin ON clin.clin_human_gene_id = hg.  hg_id
        LEFT OUTER JOIN gnomad_plof                 AS gnp  ON gnp. gnp_human_gene_id  = hg.  hg_id
        LEFT OUTER JOIN hgnc_gene                   AS hgnc ON hgnc.hgnc_human_gene_id = hg.  hg_id
        LEFT OUTER JOIN idg                                 ON idg. idg_id             = hg.  hg_id

    '''
    return spark.sql(q)


################
# mapping tables
################


def get_mouse_mapping(spark):
    get_table(spark, 'mouse_gene', 'mg_', 'id')
    get_table(spark, 'mouse_mapping_filter', 'mmf_', 'id')

    q = '''\
        SELECT mg.*, mmf.*\
        FROM mouse_gene mg\
        LEFT OUTER JOIN mouse_mapping_filter mmf ON mmf.mmf_mouse_gene_id = mg.mg_id
    '''
    return spark.sql(q)


def get_human_mapping(spark):
    get_table(spark, 'human_gene', 'hg_', 'id')
    get_table(spark, 'human_mapping_filter', 'hmf_', 'id')

    q = '''\
        SELECT hg.*, hmf.*\
        FROM human_gene hg\
        LEFT OUTER JOIN human_mapping_filter hmf ON hmf.hmf_human_gene_id = hg.hg_id
    '''
    return spark.sql(q)


def get_ortholog_mouse_mapping(spark, df_mouse_mapping):
    df_mouse_mapping.createOrReplaceTempView("mouse_mapping")

    q = '''
    SELECT o.*, mm.* FROM ortholog o
    LEFT OUTER JOIN mouse_mapping mm ON mm.mg_id = o.o_mouse_gene_id
    '''
    return spark.sql(q)


def get_ortholog_human_mapping(spark, df_human_mapping):
    df_human_mapping.createOrReplaceTempView("human_mapping")

    q = '''
    SELECT o.*, hm.* FROM ortholog o 
    LEFT OUTER JOIN human_mapping hm ON hm.hg_id = o.o_human_gene_id
    '''
    return spark.sql(q)


def get_ortholog_mouse_mapping_and_human_mapping(spark):
    q = '''
    SELECT o.*, mg.mg_mgi_gene_acc_id, mg.mg_symbol, mmf.*, hg.hg_hgnc_acc_id, hg.hg_symbol, hmf.* FROM ortholog o
    LEFT OUTER JOIN mouse_mapping_filter mmf ON mmf.mmf_id = o.o_mouse_gene_id
    LEFT OUTER JOIN human_mapping_filter hmf ON hmf.hmf_id = o.o_human_gene_id
    LEFT OUTER JOIN mouse_gene           mg  ON mg.mg_id   = o.o_mouse_gene_id
    LEFT OUTER JOIN human_gene           hg  ON hg.hg_id   = o.o_human_gene_id
    '''
    return spark.sql(q)


def get_table(spark, table_name, correlation_name, partition_column):
    if local:
        df = read_jdbc(spark, table_name, correlation_name)
    else:
        df = read_jdbc(spark, table_name, correlation_name, num_partitions=5000, column=partition_column, lower_bound=0,
                       upper_bound=999999)
    df = df.createOrReplaceTempView(table_name)
    return df


def initialise(argv):
    global jdbc_connection_str
    global essentialgenes_dir
    global ortholog_dir
    global local
    global limit
    global postgres_jdbc_jar

    jdbc_connection_str = argv[1]
    db_user = argv[2]
    db_password = argv[3]
    if len(argv) > 4 and len(argv[4]) > 0:
        essentialgenes_dir = os.path.join(argv[4], essentialgenes_dir)
        ortholog_dir = os.path.join(argv[4], ortholog_dir)
    if len(argv) > 5 and argv[5].lower() == "true":
        local = True
    if len(argv) > 6:
        limit = int(argv[6])
    if len(argv) > 7:
        postgres_jdbc_jar = argv[7]

    global properties
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }

    print('jdbc_connection_str:', jdbc_connection_str)
    print('essentialgenes directory: ', essentialgenes_dir)
    print('ortholog directory: ', ortholog_dir)
    print('local: ', local)

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
    if local:
        import findspark
        findspark.init(os.environ.get('SPARK_HOME'))
        from pyspark.sql import SparkSession
        spark = SparkSession \
            .builder \
            .config("spark.jars", postgres_jdbc_jar) \
            .master("local[*]") \
            .getOrCreate()
    else:
        from pyspark.sql import SparkSession
        spark = SparkSession \
            .builder \
            .getOrCreate()
    return spark


if __name__ == "__main__":
    sys.exit(main(sys.argv))
