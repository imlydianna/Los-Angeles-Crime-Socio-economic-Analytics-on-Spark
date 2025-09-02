from pyspark import SparkContext
import csv
from io import StringIO

# Ρύθμιση Spark και πληροφορίες job
sc = SparkContext(appName="RDD Q1 – Age Groups of Assault Victims (CSV Input)")
sc.setLogLevel("ERROR")

username = "lydiannakolitsi"
job_id = sc.applicationId
print(f"\nSpark Job ID: {job_id}")

# Διαδρομές αρχείων CSV στο HDFS
csv_2010 = "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv"
csv_2020 = "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/results/RddQ1_csv_{job_id}"

# Ανάγνωση δεδομένων ως RDD
rdd_2010 = sc.textFile(csv_2010)
rdd_2020 = sc.textFile(csv_2020)

# Ενοποίηση
rdd = rdd_2010.union(rdd_2020)

# Εξαγωγή header και φιλτράρισμα
header = rdd.first()
columns = header.split(",")
print("\nHeader columns:")
for i, col in enumerate(columns):
    print(f"{i}: {col}")

data = rdd.filter(lambda line: line != header)

# Μετατροπή σε λίστες στηλών
def parse_csv(line):
    return next(csv.reader(StringIO(line)))

rows = data.map(parse_csv)

# Στήλες που μας ενδιαφέρουν:
# Crm Cd Desc → index 9, Vict Age → index 11
# Φιλτράρισμα έγκυρων εγγραφών με μη κενά πεδία
filtered = rows.filter(lambda x: len(x) > 11 and x[9] != "" and x[11] != "") \
               .filter(lambda x: "aggravated assault" in x[9].lower())

# Συνάρτηση για κατηγοριοποίηση ηλικιών
def age_group(age):
    try:
        age = int(age)
        if age < 18:
            return "Children"
        elif age <= 24:
            return "Young Adults"
        elif age <= 64:
            return "Adults"
        else:
            return "Elderly"
    except:
        return "Unknown"

# Ομαδοποίηση ηλικιών και υπολογισμός πλήθους
result = (
    filtered.map(lambda x: (age_group(x[11]), 1))
            .filter(lambda x: x[0] != "Unknown")
            .reduceByKey(lambda a, b: a + b)
            .sortBy(lambda x: x[1], ascending=False)
)

# Εμφάνιση αποτελεσμάτων
print("\nΚατανομή ηλικιακών ομάδων θυμάτων (Aggravated Assault):")
for group, count in result.collect():
    print(f"{group}: {count}")

# Αποθήκευση σε HDFS
result.map(lambda x: f"{x[0]},{x[1]}") \
      .coalesce(1) \
      .saveAsTextFile(output_path)