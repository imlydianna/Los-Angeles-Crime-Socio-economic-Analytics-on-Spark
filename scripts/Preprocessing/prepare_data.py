# prepare_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, trim, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType

def main():
    # --- Διαμόρφωση ---
    username = "lydiannakolitsi" 

    # Δημιουργία SparkSession
    spark = SparkSession.builder \
        .appName(f"Data Preparation for {username}") \
        .getOrCreate()

    sc = spark.sparkContext
    # Μειώνουμε τα logs για να είναι πιο καθαρή η έξοδος
    sc.setLogLevel("WARN")

    print(f"Ξεκίνησε η προετοιμασία δεδομένων για τον χρήστη: {username}")

    # --- Ορισμός Διαδρομών ---
    # Είσοδος (από Πίνακα 1, είναι στο /user/root/data)
    base_input_path = "hdfs://hdfs-namenode:9000/user/root/data"
    crime_2010_input_path = f"{base_input_path}/LA_Crime_Data_2010_2019.csv"
    crime_2020_input_path = f"{base_input_path}/LA_Crime_Data_2020_2025.csv" 
    stations_input_path = f"{base_input_path}/LA_Police_Stations.csv"
    income_input_path = f"{base_input_path}/LA_income_2015.csv"
    census_input_path = f"{base_input_path}/2010_Census_Populations_by_Zip_Code.csv"
    mo_codes_input_path = f"{base_input_path}/MO_codes.txt"

    # Έξοδος (στον προσωπικό φάκελο)
    output_base_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet"

    # --- Επεξεργασία Αρχείων CSV ---

    # 1. Crime Data 2010-2019
    try:
        print(f"Επεξεργασία: {crime_2010_input_path}")
        crime_2010_df = spark.read.csv(crime_2010_input_path, header=True, inferSchema=True)
        crime_2010_output = f"{output_base_path}/crime_2010_2019"
        crime_2010_df.write.mode("overwrite").parquet(crime_2010_output)
        print(f"Ok: Το {crime_2010_output} γράφτηκε σε Parquet.")
    except Exception as e:
        print(f"Σφάλμα στην επεξεργασία Crime Data 2010-2019: {e}")

    # 2. Crime Data 2020-Present
    try:
        print(f"Επεξεργασία: {crime_2020_input_path}")
        crime_2020_df = spark.read.csv(crime_2020_input_path, header=True, inferSchema=True)
        crime_2020_output = f"{output_base_path}/crime_2020_present"
        crime_2020_df.write.mode("overwrite").parquet(crime_2020_output)
        print(f"Ok: Το {crime_2020_output} γράφτηκε σε Parquet.")
    except Exception as e:
        print(f"Σφάλμα στην επεξεργασία Crime Data 2020-Present: {e}")

    # 3. LA Police Stations
    try:
        print(f"Επεξεργασία: {stations_input_path}")
        stations_df = spark.read.csv(stations_input_path, header=True, inferSchema=True)
        stations_output = f"{output_base_path}/la_police_stations"
        stations_df.write.mode("overwrite").parquet(stations_output)
        print(f"Ok: Το {stations_output} γράφτηκε σε Parquet.")
    except Exception as e:
        print(f"Σφάλμα στην επεξεργασία LA Police Stations: {e}")

    # 4. Median Household Income
    try:
        print(f"Επεξεργασία: {income_input_path}")
        income_df = spark.read.csv(income_input_path, header=True, inferSchema=True)
        income_output = f"{output_base_path}/la_income_2015"
        income_df.write.mode("overwrite").parquet(income_output)
        print(f"Ok: Το {income_output} γράφτηκε σε Parquet.")
    except Exception as e:
        print(f"Σφάλμα στην επεξεργασία Median Household Income: {e}")

    # 5. Census Populations
    try:
        print(f"Επεξεργασία: {census_input_path}")
        census_df = spark.read.csv(census_input_path, header=True, inferSchema=True)
        census_output = f"{output_base_path}/census_2010_zipcode"
        census_df.write.mode("overwrite").parquet(census_output)
        print(f"Ok: Το {census_output} γράφτηκε σε Parquet.")
    except Exception as e:
        print(f"Σφάλμα στην επεξεργασία Census Populations: {e}")

    # --- Επεξεργασία Αρχείου MO Codes (TXT) ---
    try:
        print(f"Επεξεργασία: {mo_codes_input_path}")
        mo_codes_raw_df = spark.read.text(mo_codes_input_path)

        # Σπάσιμο κάθε γραμμής στον πρώτο κενό χαρακτήρα
        # getItem(0) -> Κωδικός, getItem(1) -> Περιγραφή
        mo_codes_df = mo_codes_raw_df.withColumn(
            "parts", split(col("value"), " ", 2)
        ).filter(col("value").isNotNull() & (col("value") != "") & (col("parts").getItem(0).isNotNull())).select( 
            trim(col("parts").getItem(0)).cast("int").alias("MO_Code"),
            trim(col("parts").getItem(1)).alias("Description")
        ).filter(col("MO_Code").isNotNull())


        mo_codes_output = f"{output_base_path}/mo_codes"
        mo_codes_df.write.mode("overwrite").parquet(mo_codes_output)
        print(f"Ok: Το {mo_codes_output} γράφτηκε σε Parquet.")
    except Exception as e:
        print(f"Σφάλμα στην επεξεργασία MO Codes: {e}")

    # --- Τέλος ---
    print("Η προετοιμασία δεδομένων ολοκληρώθηκε.")
    spark.stop()

if __name__ == "__main__":
    main()