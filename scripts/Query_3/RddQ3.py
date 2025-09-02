from pyspark.sql import SparkSession

# Ρυθμίσεις
username = "lydiannakolitsi"
spark = SparkSession.builder.appName("RDD Query 3 - Avg Income per Person").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
job_id = sc.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές
pop_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/census_2010_zipcode"
income_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/la_income_2015"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/results/RddQ3_{job_id}"

# Ανάγνωση δεδομένων
df_pop = spark.read.parquet(pop_path)
df_income = spark.read.parquet(income_path)

# Συνάρτηση εύρεσης πεδίου
def try_keys(row, options):
    for key in options:
        if key in row.asDict():
            return row[key]
    return None

# Καθαρισμός income strings
def clean_income(val):
    if val is None:
        return None
    try:
        val_str = str(val).strip().replace('$', '').replace(',', '')
        val_float = float(val_str)
        if val_float <= 0:
            return None
        return val_float
    except:
        return None

# RDD: ZipCode → Household Size
hhsize_rdd = df_pop.rdd.map(lambda row: (
    str(try_keys(row, ["Zip Code", "zip_code"])).zfill(5),
    try_keys(row, ["Average Household Size", "average_household_size"])
)).filter(lambda x: x[0] is not None and x[1] is not None) \
  .map(lambda x: (x[0], float(x[1]))) \
  .filter(lambda x: 0 < x[1] <= 15)

# RDD: ZipCode → Cleaned Median Income
income_rdd = df_income.rdd.map(lambda row: (
    str(try_keys(row, ["Zip Code", "ZipCode"])).zfill(5),
    clean_income(try_keys(row, ["Estimated Median Income", "Income"]))
)).filter(lambda x: x[0] is not None and x[1] is not None)

# Join and compute
joined = income_rdd.join(hhsize_rdd)

result = joined.map(lambda x: (
    x[0],
    round(x[1][0] / x[1][1], 2)
)).sortBy(lambda x: x[0])

# Save and Print
result.map(lambda x: f"{x[0]},{x[1]}") \
      .coalesce(1) \
      .saveAsTextFile(output_path)

print(" Ενδεικτικά αποτελέσματα:")
for item in result.take(10):
    print(f"Zip: {item[0]} → Avg Income/person: ${item[1]}")