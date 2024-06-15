from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2

spark = SparkSession.builder \
    .appName('Transformation') \
    .config('spark.jars', '/home/hansel/postgres/postgresql-42.7.3.jar') \
    .getOrCreate()

jdbc_url = 'jdbc:postgresql://localhost:5432/ecommerce'

properties = {
    'user': 'postgres',
    'password': 'admin',
    'driver': 'org.postgresql.Driver'
}

df_product = spark.read.jdbc(url=jdbc_url, table='product', properties=properties)
df_category = spark.read.jdbc(url=jdbc_url, table='category', properties=properties)
df_subcategory = spark.read.jdbc(url=jdbc_url, table='subcategory', properties=properties)

df_join = df_product.join(df_category, df_product['category_id'] == df_category['category_id'], 'inner')
df_join = df_join.join(df_subcategory, df_product['subcategory_id'] == df_subcategory['subcategory_id'], 'inner')
df_join = df_join.withColumn('unit_profit_usd', col('unit_price_usd') - col('unit_cost_usd'))

selected_columns = ['product_id', 'name', 'brand', 'unit_cost_usd', 'unit_price_usd', 'unit_profit_usd', 'category', 'subcategory']

df_join = df_join.select(*selected_columns)

jdbc_url = 'jdbc:postgresql://localhost:5432/olap_ecommerce'

df_join.write.jdbc(url=jdbc_url, table="product", mode="overwrite", properties=properties)

conn = psycopg2.connect(
    dbname="olap_ecommerce",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

sql_command = """
ALTER TABLE product
ADD CONSTRAINT pk_product_id PRIMARY KEY (product_id);
"""

cur.execute(sql_command)
conn.commit()

cur.close()
conn.close()