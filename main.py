# read a source.csv file with ^ as a delimiter
# change partition of the data to 20
# perform a  transformation on the data
# reduce the partition to 5
# write the data into a hive table called as t1 by removing the exsiting data into it.
# Schema
# category_code | security_subtype| n_net_amount_of_transaction | n_quantity |
# Transformation
# -------------------------
# CONCAT(
#       IF  ( LEFT(category_code,2) REGEXP '[0-9]{2}',
#               CONCAT('yy',SUBSTR(tr.category_code,3)),.category_code
#           )
#     , '-',
#             CASE WHEN se.security_subtype<=>'MM'
#                  THEN 'M'
#                  WHEN IFNULL(tr.security_number,'') = ''
#                  THEN 'C'
#                  ELSE 'S'
#                  END,'-',
#              CASE WHEN IF(se.security_subtype<=>'MM',-1,1) *
#                   CASE WHEN IFNULL(tr.n_net_amount_of_transaction,0)= 0
#                       THEN IFNULL(tr.n_quantity,0)
#                       ELSE IFNULL(tr.n_net_amount_of_transaction,0)
#                       END<=0
#              THEN 'C' ELSE 'D'
#              END,'-',
#              CASE WHEN IFNULL(tr.n_quantity,0)=0 THEN 'E' ELSE'N' END,'-',
#              CASE WHEN IFNULL(tr.n_net_amount_of_transaction,0)=0 THEN 'E' ELSE'N' END
#         )

# Use DF instead of SQL

# security_subtype
# WHEN col("security_subtype") != 'MM'
# THEN 'M' WHEN col("security_number") is null

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def transform(df):
    return df.withColumn('transformed_column', (concat(
        when(regexp_extract(col('category_code'), '^[0-9]{2}', 0).isNotNull(),
             concat(lit('yy'), substring(col('category_code'), 0, 3)))
            .otherwise(col('category_code')), lit('-'),
        when(col('security_subtype') == 'MM', lit('M'))
            .when(isnull(col('security_number')), lit('C'))
            .otherwise(lit('S')), lit('-'),
            when(col('security_subtype') == 'MM', -1).otherwise(1) *
            when((coalesce(col('n_net_amount_of_transaction'), lit(0)) == 0) &
                 (coalesce(col('n_quantity'), lit(0)) != 0), col('n_quantity'))
            .otherwise(coalesce(col('n_net_amount_of_transaction'), lit(0))) <= 0, lit('C')
            .otherwise(lit('D')), lit('-'),
        when(coalesce(col('n_quantity'), lit(0)) == 0, lit('E'))
            .otherwise(lit('N')), lit('-'),
        when(coalesce(col('n_net_amount_of_transaction'), lit(0)) == 0, lit('E'))
            .otherwise(lit('N'))
    )))


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('TestSparkApp') \
        .enableHiveSupport() \
        .getOrCreate()

    # define the schema for the input file
    schema = StructType([
        StructField('category_code', StringType(), True),
        StructField('security_subtype', StringType(), True),
        StructField('n_net_amount_of_transaction', DoubleType(), True),
        StructField('n_quantity', IntegerType(), True)
    ])

    # path/to/
    source_data = "source.csv"

    # read the input file into a DataFrame
    df_csv = spark.read.format('csv') \
        .option('header', True) \
        .option('delimiter', '^') \
        .schema(schema) \
        .load(source_data)

    df_partition = df_csv.repartition(20)

    df_transformed = transform(df_partition)

    df_coalesce = df_transformed.coalesce(5)

    df_coalesce.show(truncate=False)

    # to write to hive it needs to enable enableHiveSupport
    df_coalesce.write.mode('overwrite') \
        .saveAsTable("t1")
