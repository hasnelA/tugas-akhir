from pyspark.sql import SparkSession

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

df_orders = spark.read.jdbc(url=jdbc_url, table='orders', properties=properties)

jdbc_url = 'jdbc:postgresql://localhost:5432/olap_ecommerce'

df_orders.write.jdbc(url=jdbc_url, table="orders", mode="overwrite", properties=properties)