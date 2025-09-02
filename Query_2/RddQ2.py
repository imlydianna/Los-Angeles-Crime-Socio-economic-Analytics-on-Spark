from pyspark.sql import SparkSession
from datetime import datetime

# Ορισμός χρήστη και flags
username = "lydiannakolitsi"
USE_SAMPLE = False  # True για 5%, False για πλήρες dataset

spark = SparkSession.builder.appName("RDD Query 2 - Top Precincts").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
job_id = sc.applicationId

print(f"\nSpark Job ID: {job_id}")

# Διαδρομές στο HDFS
path_2010 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2010_2019"
path_2020 = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2020_present"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/results/RddQ2_{job_id}"

# Φόρτωση και Ένωση Δεδομένων
df_2010 = spark.read.parquet(path_2010)
df_2020 = spark.read.parquet(path_2020)

def trim_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    return df

df_all = trim_cols(df_2010).unionByName(trim_cols(df_2020))

# Υποσύνολο ή πλήρες dataset
if USE_SAMPLE:
    df_all = df_all.sample(False, 0.05, seed=42)
    print("Χρήση υποσυνόλου (5%)")
else:
    print("Χρήση πλήρους συνόλου")

# Μετατροπή σε RDD και βασικό φιλτράρισμα
rdd = df_all.select("DATE OCC", "AREA NAME", "Status Desc", "LAT", "LON").rdd \
    .filter(lambda row: row["DATE OCC"] and row["AREA NAME"] and row["Status Desc"]) \
    .filter(lambda row: not (row["LAT"] == 0.0 and row["LON"] == 0.0))  # Αποφυγή Null Island

# Συνάρτηση εξαγωγής έτους από ημερομηνία
def extract_year(date_str):
    try:
        return int(date_str.split("/")[2][:4])
    except:
        return None

# Συνάρτηση για κλειστή υπόθεση
def is_closed(status_desc):
    return status_desc not in ("UNK", "Invest Cont")

# Χαρτογράφηση: ((year, precinct), (1, closed_flag))
year_precinct_flags = rdd.map(lambda row: (
    (extract_year(row["DATE OCC"]), row["AREA NAME"]),
    (1, 1 if is_closed(row["Status Desc"]) else 0)
)).filter(lambda x: x[0][0] is not None)

# Aggregation: συνολικός αριθμός και κλειστές υποθέσεις ανά (year, precinct)
agg = year_precinct_flags.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Μετατροπή: (year, (precinct, closed_case_rate %))
year_to_precincts = agg.map(lambda x: (
    x[0][0],  # year
    (x[0][1], (x[1][1] / x[1][0]) * 100)  # (precinct, closed_case_rate %)
)).groupByKey()

# Top-3 precincts ανά έτος με rank
def top3_with_rank(group):
    year, precincts = group
    sorted_top = sorted(precincts, key=lambda x: -x[1])[:3]
    return [(year, p[0], p[1], i+1) for i, p in enumerate(sorted_top)]

result = year_to_precincts.flatMap(top3_with_rank).sortBy(lambda x: (x[0], x[3]))

# Εμφάνιση αποτελεσμάτων
print("\n Top-3 precincts ανά έτος με βάση το ποσοστό κλειστών υποθέσεων:")
for row in result.collect():
    print(f"{row[0]}\t{row[1]}\t{row[2]:.2f}\t#{row[3]}")

# Αποθήκευση στο HDFS
result.map(lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]}") \
      .coalesce(1) \
      .saveAsTextFile(output_path)
