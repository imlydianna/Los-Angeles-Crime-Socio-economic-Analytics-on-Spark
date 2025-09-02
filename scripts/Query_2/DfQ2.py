from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, to_timestamp, count, sum as _sum, dense_rank
from pyspark.sql.window import Window

# Ρύθμιση Spark και πληροφορίες job
spark = SparkSession.builder.appName("DF Q2 – Top-3 Precincts per Year").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "lydiannakolitsi"
USE_SAMPLE = False  # False για πλήρες dataset

job_id = sc.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές HDFS
path_2010 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2010_2019"
path_2020 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2020_present"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/results/DfQ2_{job_id}"

# Κανονικοποίηση ονομάτων στηλών
def trim_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    return df

# Ανάγνωση και ένωση των δεδομένων
df_2010 = trim_cols(spark.read.parquet(path_2010))
df_2020 = trim_cols(spark.read.parquet(path_2020))
df = df_2010.unionByName(df_2020)

# Sample ή πλήρες dataset
if USE_SAMPLE:
    print("Τρέχει με υποσύνολο (sample 5%)")
    df = df.sample(withReplacement=False, fraction=0.05, seed=42)
else:
    print("Τρέχει με ολόκληρο το dataset")

# Φιλτράρισμα σχετικών εγγραφών
df = df.filter(
    (col("DATE OCC").isNotNull()) &
    (col("AREA NAME").isNotNull()) &
    (col("Status Desc").isNotNull()) &
    ~((col("LAT") == 0.0) & (col("LON") == 0.0))  # Αποφυγή Null Island
)

# Εξαγωγή έτους και επισήμανση "κλειστών" υποθέσεων
df = df.withColumn("Year", year(to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))) \
                         .withColumn("Closed", when(~col("Status Desc").isin("UNK", "Invest Cont"), 1).otherwise(0))

# Ομαδοποίηση και υπολογισμός ποσοστού
agg = df.groupBy("Year", "AREA NAME") \
                 .agg(
                     count("*").alias("Total"),
                     _sum("Closed").alias("Closed")
                 ) \
                 .withColumn("closed_case_rate", (col("Closed") / col("Total")) * 100)


# Ανάκτηση Top-3 τμημάτων ανά έτος
window = Window.partitionBy("Year").orderBy(col("closed_case_rate").desc())

result = agg.withColumn("Rank", dense_rank().over(window)) \
            .filter(col("Rank") <= 3) \
            .orderBy("Year", "Rank")

# Εμφάνιση αποτελεσμάτων με format (όπως στην εικόνα)
print("\nΚατανομή Top-3 precincts ανά έτος με βάση το ποσοστό κλεισμένων υποθέσεων:")
rows = result.select("Year", "AREA NAME", "closed_case_rate", "Rank").collect()
for row in rows:
    year = row["Year"]
    precinct = row["AREA NAME"]
    rate = f"{row['closed_case_rate']:.2f}"
    rank = f"#{row['Rank']}"
    print(f"{year:<6} {precinct:<20} {rate:<20} {rank}")


# Αποθήκευση στο HDFS
result.select("Year", "AREA NAME", "closed_case_rate", "Rank") \
         .coalesce(1) \
         .write.mode("overwrite") \
         .option("header", True) \
         .csv(output_path)

# Εμφάνιση φυσικού πλάνου
#print("\nPhysical Plan:")
#result.explain(True)
