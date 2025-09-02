from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, split, explode, trim, sqrt, pow, avg, count, 
    row_number, rank, 
    radians, sin, cos, atan2, # Για Haversine
    round as spark_round
)
from pyspark.sql.window import Window

# Ρύθμιση Spark
username = "lydiannakolitsi"
spark = SparkSession.builder.appName("DF Query 4 - Haversine Distance to Closest Precinct").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
job_id = spark.sparkContext.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές HDFS
crime_2010_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2010_2019"
crime_2020_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_2020_present"
mo_codes_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/mo_codes"
stations_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/la_police_stations"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/results/DfQ4_haversine_{job_id}"

# Καθαρισμός ονομάτων στηλών
def trim_colnames(df):
    new_columns = [c.strip() for c in df.columns]
    if new_columns == df.columns:
        return df
    return df.select([col(original_name).alias(new_name)
                      for original_name, new_name in zip(df.columns, new_columns)])

# Ανάγνωση και προεπεξεργασία
df_2010 = trim_colnames(spark.read.parquet(crime_2010_path))
df_2020 = trim_colnames(spark.read.parquet(crime_2020_path))
crime_df_initial = df_2010.unionByName(df_2020)

mo_df = trim_colnames(spark.read.parquet(mo_codes_path))
stations_df = trim_colnames(spark.read.parquet(stations_path))

# Εξαγωγή weapon-related MO codes
weapon_mo = mo_df.filter(
    lower(col("Description")).contains("gun") | 
    lower(col("Description")).contains("weapon")
).select(col("MO_Code").cast("int").alias("code"))

# Ανάλυση MOcodes και φιλτράρισμα περιστατικών με όπλα
crime_df_exploded_mocodes = crime_df_initial.filter(col("Mocodes").isNotNull()) \
                                   .withColumn("Mocodes_split", split(col("Mocodes"), " ")) \
                                   .withColumn("MO_Code_str", explode(col("Mocodes_split"))) \
                                   .withColumn("MO_Code", col("MO_Code_str").cast("int"))

# Join με weapon_mo για να πάρουμε μόνο τα weapon related crimes
crime_weapon_df = crime_df_exploded_mocodes.join(weapon_mo, crime_df_exploded_mocodes["MO_Code"] == weapon_mo["code"], "inner") \
                                     .filter(col("LAT").isNotNull() & col("LON").isNotNull()) \
                                     .filter(~((col("LAT") == 0.0) & (col("LON") == 0.0))) \
                                     .select("DR_NO", "LAT", "LON") \
                                     .distinct() # Για να έχουμε ένα DR_NO ανά τοποθεσία αν ένα έγκλημα έχει πολλαπλούς weapon mocodes

# Επιλογή συντεταγμένων περιστατικών
crime_locations = crime_weapon_df.select(
    col("DR_NO"),
    col("LAT").cast("float").alias("crime_lat"),
    col("LON").cast("float").alias("crime_lon")
)

# Συντεταγμένες LAPD σταθμών (σε μοίρες) 
stations = stations_df.select(
    trim(col("DIVISION")).alias("division"),
    col("Y").cast("float").alias("station_lat"),
    col("X").cast("float").alias("station_lon")
)

# Cartesian Join μεταξύ περιστατικών και τμημάτων 
crime_cross = crime_locations.crossJoin(stations)

# Υπολογισμός απόστασης Haversine
R = 6371.0  # Ακτίνα της Γης σε χιλιόμετρα

# Μετατροπή μοιρών σε ακτίνια
crime_lat_rad = radians(col("crime_lat"))
crime_lon_rad = radians(col("crime_lon"))
station_lat_rad = radians(col("station_lat"))
station_lon_rad = radians(col("station_lon"))

# Υπολογισμός Δφ και Δλ
dlat = station_lat_rad - crime_lat_rad
dlon = station_lon_rad - crime_lon_rad

# Haversine formula
a = sin(dlat / 2)**2 + cos(crime_lat_rad) * cos(station_lat_rad) * sin(dlon / 2)**2
c = 2 * atan2(sqrt(a), sqrt(1 - a))
distance_km_col = R * c

crime_distances = crime_cross.withColumn("distance_km", distance_km_col)

# Εύρεση πλησιέστερου τμήματος για κάθε περιστατικό (με βάση το DR_NO)
window_spec = Window.partitionBy("DR_NO").orderBy("distance_km") # Ταξινόμηση με τη νέα στήλη απόστασης
nearest = crime_distances.withColumn("rank_val", rank().over(window_spec)).filter(col("rank_val") == 1)

# Ομαδοποίηση ανά division
result = nearest.groupBy("division").agg(
    spark_round(avg("distance_km"), 3).alias("avg_distance_km"), # H μέση απόσταση είναι ήδη σε km
    count("*").alias("count")
).orderBy(col("count").desc())

# Εμφάνιση πλάνου εκτέλεσης Catalyst
print("\nCatalyst Physical Plan\n")
result.explain(mode="formatted")

# Αποθήκευση στο HDFS
result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

# Εμφάνιση αποτελεσμάτων 
print(" Top LAPD divisions με εγκλήματα που σχετίζονται με όπλα (απόσταση Haversine σε km):")
result.select("division", "avg_distance_km", "count").show(truncate=False)