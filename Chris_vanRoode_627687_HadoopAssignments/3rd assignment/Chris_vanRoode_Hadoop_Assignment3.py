from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import findspark

findspark.init()

spark = SparkSession.builder.master("local").appName("Linear Regression Model").config("spark.executor.memory", "1gb").getOrCreate()    
sc = spark.sparkContext

# Load data from spark into dataframe
df = spark.read.format("CSV").option("header", "true").load(".\\TitanicCSV\\titanic.csv")

df = df.withColumn("Survived", df["Survived"].cast(IntegerType())) \
    .withColumn("Class", df["Pclass"].cast(IntegerType())) \
    .withColumn("Name", df["Name"].cast(StringType())) \
    .withColumn("Sex", df["Sex"].cast(StringType())) \
    .withColumn("Age", df["Age"].cast(IntegerType())) \
    .withColumn("Siblings/Spouses Aboard", df["Siblings/Spouses Aboard"].cast(IntegerType())) \
    .withColumn("Parents/Children Aboard", df["Parents/Children Aboard"].cast(IntegerType())) \
    .withColumn("PayedFare", df["Fare"].cast(FloatType())) \

# A
# Extract the survival persons' sex and class
survived = df.select("Survived", "Sex", "Class")

# Group classes and sexes to see who survived
survived = survived.groupBy("Sex", "Class").avg("Survived")
survived.show()

# B
# Get a new dataframe with only Age, Class and Survived
childs = df.select("Age", "Class", "Survived").toPandas()

# Filter survived with age under 10 and class 3
childs = childs[(childs['Age'] <= 10) & (childs['Class'] == 3)][['Age', 'Survived', 'Class']]

# See how many kids class 3 age < 10 survived
childsSurvived = childs[childs['Survived'] == 1].count()[0]

# Predict whats the chance of a child under 10 and the 3 class surviving
probability = (childsSurvived / childs.count()[0] * 100).item()
#Shows the results
print("Chance of survival: " + str(probability) + "%")

# C
# Create a new dataframe with the class and payedFare 
payed = df.select("Class", "PayedFare")

# See how many passengers payed according to each class
payed = payed.groupBy("Class").avg("PayedFare")

#Shows the results
payed.show()