from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, round as spark_round
from pyspark.sql.types import FloatType

# Ρύθμιση
username = "lydiannakolitsi"
spark = SparkSession.builder.appName("DF Query 3 - Avg Income per Person").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
job_id = sc.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές
pop_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/census_2010_zipcode"
income_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/la_income_2015"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/results/DfQ3_{job_id}"

# Ανάγνωση και καθαρισμός δεδομένων

# Δεδομένα πληθυσμού
df_pop = spark.read.parquet(pop_path) \
    .select(
        col("Zip Code").cast("string").alias("zip"),
        col("Average Household Size").cast("float").alias("household_size")
    ).filter("household_size > 0")

# Δεδομένα εισοδήματος
df_income = spark.read.parquet(income_path) \
    .select(
        regexp_replace(regexp_replace(col("Estimated Median Income"), "\\$", ""), ",", "")
        .cast(FloatType()).alias("income"),
        col("Zip Code").cast("string").alias("zip")
    ).filter("income > 0")

# Join και Υπολογισμός
df_joined = df_income.join(df_pop, on="zip", how="inner")
df_joined.explain(mode="formatted")

df_result = df_joined.withColumn(
    "income_per_person", spark_round(col("income") / col("household_size"), 2)
).select("zip", "income_per_person").orderBy("zip")

# Αποθήκευση
df_result.selectExpr("concat(zip, ',', income_per_person)").coalesce(1) \
    .write.mode("overwrite").text(output_path)

# Εμφάνιση ενδεικτικών αποτελεσμάτων
print(" Ενδεικτικά αποτελέσματα:")
df_result.show(10)
