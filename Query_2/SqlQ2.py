from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year

# Ρύθμιση Spark και πληροφορίες job
spark = SparkSession.builder.appName("SQL Q2 – Top-3 Precincts per Year").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

username = "lydiannakolitsi"
USE_SAMPLE = False  # False για πλήρες dataset

job_id = sc.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές HDFS
path_2010 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2010_2019"
path_2020 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2020_present"
output_dir  = f"hdfs://hdfs-namenode:9000/user/{username}/results/SqlQ2_{job_id}"

# Κανονικοποίηση ονομάτων στηλών
def trim_cols(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip())
    return df

# Ανάγνωση και ένωση των δεδομένων
df_2010 = trim_cols(spark.read.parquet(path_2010))
df_2020 = trim_cols(spark.read.parquet(path_2020))
df = df_2010.unionByName(df_2020)

# Sample ή πλήρες dataset
if USE_SAMPLE:
    print("Τρέχει με υποσύνολο (sample 5%)")
    df = df.sample(False, 0.05, seed=42)
else:
    print("Τρέχει με ολόκληρο το dataset")

# Προετοιμασία για SQL: εξαγωγή έτους και προσωρινό view
df = df.filter(
    (df["DATE OCC"].isNotNull()) &
    (df["AREA NAME"].isNotNull()) &
    (df["Status Desc"].isNotNull()) &
    ~((df["LAT"] == 0.0) & (df["LON"] == 0.0)) # Αποφυγή Null Island
).withColumn("Year", year(to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")))

df.createOrReplaceTempView("crime")

# SQL ερώτημα για υπολογισμό ποσοστών και επιλογή Top-3 ανά έτος
query = """
WITH base AS (
    SELECT
        Year,
        `AREA NAME` AS precinct,
        COUNT(*) AS Total,
        SUM(CASE WHEN `Status Desc` NOT IN ('UNK', 'Invest Cont') THEN 1 ELSE 0 END) AS Closed
    FROM crime
    GROUP BY Year, `AREA NAME`
),
rates AS (
    SELECT
        Year,
        precinct,
        ROUND(100.0 * Closed / Total, 10) AS closed_case_rate
    FROM base
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Year ORDER BY closed_case_rate DESC) AS rank
    FROM rates
)
SELECT Year, precinct, closed_case_rate, rank
FROM ranked
WHERE rank <= 3
ORDER BY Year ASC, rank ASC
"""

# Εκτέλεση SQL
result = spark.sql(query)

# Εμφάνιση αποτελεσμάτων 
print("\nTop-3 precincts ανά έτος με βάση το ποσοστό κλεισμένων υποθέσεων:")
rows = result.collect()
for row in rows:
    year = row["Year"]
    precinct = row["precinct"]
    rate = f"{row['closed_case_rate']:.10f}"
    rank = f"#{row['rank']}"
    print(f"{year:<6} {precinct:<20} {rate:<20} {rank}")

# Αποθήκευση στο HDFS
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_dir)