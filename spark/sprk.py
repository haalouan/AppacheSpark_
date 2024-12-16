import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Spark").getOrCreate()
dataFrame = spark.read.format('csv').option('inferSchema', True).option('header', True).load('data.csv')
ddlSchema = '''
    User_ID string ,
    Product_ID string ,
     Category string ,
     Price string,
     Discount integer ,
     Final_Price double ,
     Payment_Method string ,
     Purchase_Date string
     '''
dataFrame = spark.read.format('csv').schema(ddlSchema).option('header', True).load('data.csv') #changing the schema
dataFrame.printSchema()
dataFrame.select('User_ID', 'Product_ID', 'Category').show() #selecting what to show
dataFrame.select(dataFrame['User_ID'].alias('Item_ID')).show() #alias change the name
dataFrame.filter(dataFrame['Category']=='Sports').show() #filtering the data scenario 1
dataFrame.filter((dataFrame['Price']<100) & (dataFrame['Category']=='Books')).show() #filtering the data scenario 2
dataFrame.filter((dataFrame['Price']<100) & (dataFrame['Category']=='Books') & (dataFrame['Payment_Method']=='UPI')).show() #filtering the data scenario 3
dataFrame.withColumnRenamed('User_ID','Item_ID').show() #changing the name of the column
dataFrame.withColumn('Name', lit("new")) #creating new column with name 'Name' with constant 'new'
dataFrame.withColumn("Category", lit("HAHA")).show() #changing the existing column "Category" with the constant "haha"
dataFrame.withColumn('Multiple', dataFrame['Price']*dataFrame['Discount']).show() # creating new column 'Multiple' and put in it the multiple of the column 'Price' with the column 'Discount'
dataFrame.withColumn('Category', regexp_replace('Category', "Sports", "spts")).show() #changing every value "Sports" in an existing column named "Category"
dataFrame.withColumn('Price', dataFrame['Price'].cast(IntegerType())).show() #cast the type of a column
dataFrame.sort('Price', ascending=False).show() #sort descending
dataFrame.sort('Final_Price').show() #sort ascending
dataFrame.sort('Price', 'Discount', ascending=[0,1]).show() #sort multiple columns
dataFrame.limit(10).show() #show only first 10 lines
dataFrame.drop('Final_Price').show() #remove a column
dataFrame.drop('Final_Price','Purchase_Date').show() #remove 2 columns
dataFrame.dropDuplicates().show() #remove the duplicates from all the columns
dataFrame.distinct().show() #same as dropDuplicate
dataFrame.dropDuplicates(subset=['Category']).show() #remove the duplicate from one column
data1=[(1, "A"),
        (2, 'B')]
data2=[("C", 3),
        ('D', 4)]
schema1 = StructType().add("id", IntegerType()).add("name", StringType())
schema2 = StructType().add("name", StringType()).add("id", IntegerType())
df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)
df1.show()
df2.show()
df1.union(df2).show() #work if the 2 datas are in the same levels
df2.union(df1).show() #in this case will be a problem of the data beeing in the wrong column
df1.unionByName(df2).show() #now no miss abnd all the data are in the correct column
dataFrame.select(initcap('Category')).show()
dataFrame.select(lower('Category')).show()
dataFrame.select(upper('Category')).show()
#date
dataFrameDate = [()]
dataFrameDate = spark.createDataFrame(dataFrameDate)
dataFrameDate = dataFrameDate.withColumn('Current_Date', current_date())#current date
dataFrameDate.show()
dataFrameDate = dataFrameDate.withColumn('Week_After', date_add('Current_Date', 7))#add 7 days
dataFrameDate.show()
dataFrameDate = dataFrameDate.withColumn('Week_Before', date_sub('Current_Date', 7))#sub 7 days
dataFrameDate.show()
dataFrameDate = dataFrameDate.withColumn('Week_Before', date_add('Current_Date', -7))#sub 7 days
dataFrameDate.show()
dataFrameDate = dataFrameDate.withColumn('Datediff-', datediff('Current_Date', 'Week_After'))#the date btw current and after
dataFrameDate.show()
dataFrameDate = dataFrameDate.withColumn('Datediff+', datediff('Current_Date', 'Week_Before'))#the date btw current and before
dataFrameDate.show()
dataFrameDate = dataFrameDate.withColumn('Week_Before', date_format('Week_Before','dd-mm-yyyy'))#change the format of the date
dataFrameDate.show()
#handling nulls data with dropna
dataFrame.dropna('all').show() #remove all the row wish have all the columns are null
dataFrame.dropna('any').show() #remove any row wish have any null in any column
dataFrame.dropna(subset=['Category']).show() #remove the row wish have null in only the column 'Category'
#handling nulls data with fillna
dataFrame.fillna('NotAvailabe').show() #fill the null data
