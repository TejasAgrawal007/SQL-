# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC **Data Reading JSON**

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema', True)\
                                .option('header', True)\
                                .option("multiLine", True)\
                                .load("/FileStore/tables/drivers.json")

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading Utils**

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).load("/FileStore/tables/BigMart_Sales.csv")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Definition**

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **DDL SCHEMA**

# COMMAND ----------

my_ddl_schema = ''' Item_Identifier String,
                    Item_Weight String,
                    Item_Fat_Content String,
                    Item_Visibility double,
                    Item_Type String,
                    Item_MRP Double,
                    Outlet_Identifier String,
                    Outlet_Establishment_Year int,
                    Outlet_Size String,
                    Outlet_Location_Type String,
                    Outlet_Type String, 
                    Item_Outlet_Sales Double '''


# COMMAND ----------

df = spark.read.format("csv")\
                        .option('header', True)\
                        .schema(my_ddl_schema)\
                        .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **SELECT**

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import * 
from pyspark.sql.functions import *  

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(col("Item_Identifier"), col("Item_Weight"), col("Item_Type")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **ALIAS**

# COMMAND ----------

df.select(col("Item_Identifier").alias("Item_id")).display()


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## FILTER

# COMMAND ----------

# MAGIC %md 
# MAGIC ***Scenario - 1***

# COMMAND ----------

df.filter(col("Item_Fat_Content") == "Regular").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ***Scenario - 2***

# COMMAND ----------

df.filter((col("Item_Type") == "Soft Drinks") & (col("Item_Weight") < 10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ***Scenario - 3***

# COMMAND ----------

df.filter((col("Outlet_Size").isNull() & col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_withColumnRenamed_**

# COMMAND ----------

df.withColumnRenamed("Item_Weight", "Tejas").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC **_Scenario - 1_**

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df = df.withColumn('flag', lit("new"))
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 2**

# COMMAND ----------

df = df.withColumn("Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "reg", "Tejas"))\
    .withColumn("Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "LF", "Krishna"))
df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type Casting

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("Item_Visibility", col("Item_Visibility").cast(StringType()))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 1**

# COMMAND ----------

df.sort(col("Item_Weight").desc()).limit(5).display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 2**

# COMMAND ----------

df.sort(col("Item_Visibility").asc()).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 3**

# COMMAND ----------

# MAGIC %md
# MAGIC _**Value_ | Meaning
# MAGIC 0 | Descending order (biggest to smallest)
# MAGIC 1 | Ascending order (smallest to biggest)**_

# COMMAND ----------

df.sort(["Item_Weight", "Item_Visibility"], ascending = [0,0]).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 4**

# COMMAND ----------

df.sort(["Item_Weight", "Item_Visibility"], ascending = [0,1]).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## DROP

# COMMAND ----------

# MAGIC %md 
# MAGIC **Scenario-1**

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df.drop("Item_Visibility").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario-2**

# COMMAND ----------

df.drop("Item_Visibility", "Item_Type").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **DRop_Duplicates**

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 2**

# COMMAND ----------

df.drop_duplicates(subset=["Item_Type"]).display()

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## UNION and UNION BY NAME

# COMMAND ----------

# MAGIC %md
# MAGIC **Preaparing Dataframes**

# COMMAND ----------

data1 = [
    (1, "Krishna"),
    (2, "Tejas")
]
schema1 = "id int, name String"

df1 = spark.createDataFrame(data1, schema1)

data2 = [
    (3, "Disha"),
    (4, "Dipti"),
]

schema2 = "id int, name String"

df2 = spark.createDataFrame(data1, schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Union**

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('Akash','1',),
        ('Tejas','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## String Functions

# COMMAND ----------

# MAGIC %md 
# MAGIC **Initcap()**

# COMMAND ----------

df.select(upper("Item_Fat_Content").alias("UpperCase")).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Date Functions

# COMMAND ----------

df = df.withColumn("curr_date", current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Current_Date**

# COMMAND ----------

df = df.withColumn("curr_date", current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Date_Add**

# COMMAND ----------

df = df.withColumn("week_after", date_add("curr_date", 7)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Date_Sub()**

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7)) 
df.display()
     

# COMMAND ----------

# MAGIC %md
# MAGIC ## DateDIFF

# COMMAND ----------


df = df.withColumn('datediff',datediff('week_after','curr_date'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping NUlls**

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna("any").display()

# COMMAND ----------

df.dropna(subnet=["Outlet_Identifier"]).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Filling Nulls

# COMMAND ----------

df.fillna('NotAvailable',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPLIT and Indexing

# COMMAND ----------

df.withColumn("Outlet_Type", split("Outlet_Type", ' ')).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Indexing**

# COMMAND ----------

df.withColumn("Outlet_Type", split("Outlet_Type", ' ')[1]).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Explode**

# COMMAND ----------

df_exp = df.withColumn("Outlet_Type", split("Outlet_Type", ' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn("Outlet_Type", explode("Outlet_Type")).limit(5).display()

# COMMAND ----------

df_exp.withColumn("Type1_flag",array_contains("Outlet_Type", "Type1")).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## GroupBY
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **_Scenario - 1_**

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df.groupBy("Item_Type").agg(sum("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_Scenario - 2_**

# COMMAND ----------

df.groupBy("Item_Type").agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_SCenario - 3_**

# COMMAND ----------

df.groupBy("Item_Type", "Outlet_Size").agg(sum("Item_MRP").alias("Total_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_Scenario - 4_**

# COMMAND ----------

df.groupBy("Item_Type", "Outlet_Size").agg(sum("Item_MRP"), avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect_List

# COMMAND ----------


data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data, schema)

df_book.display()

# COMMAND ----------

df_book.groupBy("user").agg(collect_list("book")).display()

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df.groupBy("Item_Type").pivot("Outlet_Size").agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## When-Otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC **_Scenario - 1_**

# COMMAND ----------

df = df.withColumn("veg_flag", when(col("Item_Type") == "Meat", "Non-Veg").otherwise("Veg"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOINS

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_Inner Join_**

# COMMAND ----------

df1.join(df2, df1["dept_id"] == df2["dept_id"], "inner").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_Left Join_**

# COMMAND ----------

df1.join(df2, df1["dept_id"] == df2["dept_id"], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_Right JOIN_**

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_ANTI JOIN_**

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## WINDOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC **_ROW_NUMBER()_**

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

from pyspark.sql.window import Window


# COMMAND ----------

df.withColumn("rowCol", row_number().over(Window.orderBy("Item_Identifier"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **_RANK VS DENSE RANK_**

# COMMAND ----------

df.withColumn("rank", rank().over(Window.orderBy(col("Item_Identifier").desc())))\
    .withColumn("denseRank", dense_rank().over(Window.orderBy(col("Item_Identifier")))).display()

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cumulative Sum

# COMMAND ----------

df.withColumn("cumsum", sum("Item_MRP").over(Window.orderBy("Item_Type"))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()


# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()


# COMMAND ----------

