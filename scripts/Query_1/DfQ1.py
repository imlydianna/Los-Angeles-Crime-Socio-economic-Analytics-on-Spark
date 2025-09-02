from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Ρύθμιση Spark και πληροφορίες job
spark = SparkSession.builder.appName("DF Q1 – Age Groups of Assault Victims").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "lydiannakolitsi"
USE_SAMPLE = False  # False για πλήρες dataset

job_id = sc.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές HDFS
path_2010 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2010_2019"
path_2020 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2020_present"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/results/DfQ1_{job_id}"

# Κανονικοποίηση ονομάτων στηλών
def trim_colnames(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip())
    return df

# Ανάγνωση και ένωση των δεδομένων
df_2010 = trim_colnames(spark.read.parquet(path_2010))
df_2020 = trim_colnames(spark.read.parquet(path_2020))
df = df_2010.unionByName(df_2020)

# Sample ή πλήρες dataset
if USE_SAMPLE:
    print("Τρέχει με υποσύνολο (sample 5%)")
    df = df.sample(withReplacement=False, fraction=0.05, seed=42)
else:
    print("Τρέχει με ολόκληρο το dataset")

# Φιλτράρισμα σχετικών εγγραφών
df = df.filter(
    (col("Crm Cd Desc").isNotNull()) &
    (col("Vict Age").isNotNull()) &
    (col("Crm Cd Desc").rlike("(?i)aggravated assault"))
)

# Ορισμός κατηγοριών ηλικίας
df = df.withColumn(
    "AgeGroup",
    when(col("Vict Age") < 18, "Children")
    .when(col("Vict Age") <= 24, "Young Adults")
    .when(col("Vict Age") <= 64, "Adults")
    .otherwise("Elderly")
    # Δεν υπάρχει .otherwise("Unknown") εδώ, καθώς το groupBy θα αγνοήσει τα nulls στην AgeGroup
)

# Υπολογισμός πλήθους ανά κατηγορία
result = df.groupBy("AgeGroup").count().orderBy(col("count").desc())

# Εμφάνιση αποτελεσμάτων
print("\n Κατανομή ηλικιακών ομάδων θυμάτων σε aggravated assault:")
result.show()

# Αποθήκευση στο HDFS
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_dir)

# Εμφάνιση φυσικού πλάνου (explain)
print("\nPhysical Plan:")
result.explain(True)
