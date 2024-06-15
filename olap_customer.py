from pyspark.sql import SparkSession
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

df_customer = spark.read.jdbc(url=jdbc_url, table='customer', properties=properties)
df_region = spark.read.jdbc(url=jdbc_url, table='region', properties=properties)

df_join = df_customer.join(df_region,
                           (df_customer['city'] == df_region['city']) &
                           (df_customer['state'] == df_region['state']),
                           'left') \
                           .select(df_customer['*'], df_region['country'])\
                           .orderBy(df_customer['customer_id'].cast("int"))

jdbc_url = 'jdbc:postgresql://localhost:5432/olap_ecommerce'

df_join.write.jdbc(url=jdbc_url, table="customer", mode="overwrite", properties=properties)

conn = psycopg2.connect(
    dbname="olap_ecommerce",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

sql_command = """
ALTER TABLE customer
ADD CONSTRAINT pk_customer_id PRIMARY KEY (customer_id);
"""

cur.execute(sql_command)
conn.commit()

cur.close()
conn.close()