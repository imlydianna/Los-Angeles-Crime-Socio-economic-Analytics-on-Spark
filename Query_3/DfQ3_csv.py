from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, round

# Ρύθμιση περιβάλλοντος
username = "lydiannakolitsi"
spark = SparkSession.builder.appName("DF Query 3 - CSV").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
job_id = sc.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές CSV αρχείων
pop_csv_path = "hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv"
income_csv_path = "hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/results/DfQ3_csv_{job_id}"

# Ανάγνωση CSV
df_pop = spark.read.option("header", True).option("inferSchema", True).csv(pop_csv_path)
df_income = spark.read.option("header", True).option("inferSchema", True).csv(income_csv_path)

print(f" Στήλες πληθυσμού: {df_pop.columns}")
print(f" Στήλες εισοδήματος: {df_income.columns}")

# Καθαρισμός εισοδήματος (αφαίρεση $ και ,)
df_income_clean = df_income.withColumn(
    "Cleaned_Income",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("float")
)

# Join και υπολογισμός
joined = df_pop.join(df_income_clean, on="Zip Code", how="inner")
joined.explain(mode="formatted")

result = joined.withColumn(
    "avg_income_per_person",
    round(col("Cleaned_Income") / col("Average Household Size"), 2)
).select("Zip Code", "avg_income_per_person").orderBy("Zip Code")

# Αποθήκευση αποτελεσμάτων
result.write.mode("overwrite").option("header", "false").csv(output_path)

# Εμφάνιση ενδεικτικών αποτελεσμάτων
print(" Ενδεικτικά αποτελέσματα:")
result.show(10)