import math
# import numpy as np
import math
# import numpy as np
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Spark").getOrCreate()
#data transformation
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
#filtering the data
dataFrame.select('User_ID', 'Product_ID', 'Category').show() #selecting what to show
dataFrame.select(dataFrame['User_ID'].alias('Item_ID')).show() #alias change the name
dataFrame.filter(dataFrame['Category']=='Sports').show() #filtering the data scenario 1
dataFrame.filter((dataFrame['Price']<100) & (dataFrame['Category']=='Books')).show() #filtering the data scenario 2
dataFrame.filter((dataFrame['Price']<100) & (dataFrame['Category']=='Books') & (dataFrame['Payment_Method']=='UPI')).show() #filtering the data scenario 3
#withColumn
dataFrame.withColumnRenamed('User_ID','Item_ID').show() #changing the name of the column
dataFrame.withColumn('Name', lit("new")) #creating new column with name 'Name' with constant 'new'
dataFrame.withColumn("Category", lit("HAHA")).show() #changing the existing column "Category" with the constant "haha"
dataFrame.withColumn('Multiple', dataFrame['Price']*dataFrame['Discount']).show() # creating new column 'Multiple' and put in it the multiple of the column 'Price' with the column 'Discount'
dataFrame.withColumn('Category', regexp_replace('Category', "Sports", "spts")).show() #changing every value "Sports" in an existing column named "Category"
dataFrame.withColumn('Price', dataFrame['Price'].cast(IntegerType())).show() #cast the type of a column
#sort
dataFrame.sort('Price', ascending=False).show() #sort descending
dataFrame.sort('Final_Price').show() #sort ascending
dataFrame.sort('Price', 'Discount', ascending=[0,1]).show() #sort multiple columns
dataFrame.limit(10).show() #show only first 10 lines
#handle null data
dataFrame.drop('Final_Price').show() #remove a column
dataFrame.drop('Final_Price','Purchase_Date').show() #remove 2 columns
dataFrame.dropDuplicates().show() #remove the duplicates from all the columns
dataFrame.distinct().show() #same as dropDuplicate
dataFrame.dropDuplicates(subset=['Category']).show() #remove the duplicate from one column
#union 2 dataSets
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
#split and indexing
newDataFrame = dataFrame.filter(dataFrame['Payment_Method']=='Net Banking')
newDataFrame.show()
newDataFrameSplit = newDataFrame.withColumn('Payment_Method', split('Payment_Method', ' ')) #spliting the data 'Net Banking' into [Net, Banking]
newDataFrame.withColumn('Payment_Method', split('Payment_Method', ' ')[1]).show() #spliting the data 'Net Banking' into [Net, Banking] and put only the index one wish is 'Banking'
# explode
newDataFrameSplit.withColumn('Payment_Method', explode('Payment_Method')).show() #explode the [Net, Bnaking] in 2 rows 'Net' in a row and 'Banking' in a row
# array_contains (check in an array if somthing exist or not)
newDataFrameSplit.withColumn('Flag', array_contains('Payment_Method', 'Net')).show() #create a new column 'flag' and put int it true or false if the 'Net' is there or not
#***********************************[group by] the most popular function in data world ***************************************************************************
#senario 1 (1 column)
dataFrame.groupBy('Category').agg(sum('Price')).show() #the sum
dataFrame.groupBy('Category').agg(mean('Price')).show() #the mean
dataFrame.groupBy('Category').agg(avg('Price')).show() #the average
#senario 2 (multiple columns)
dataFrame.groupBy('Category', 'Product_ID').agg(sum('Price'), mean('Price'), sum('Discount'), mean('Discount')).show()
#advanced pyspark
data =[
        ('user1', 'book1'),
        ('user1', 'book2'),
        ('user2', 'book2'),
        ('user2', 'book4'),
        ('user3', 'book1'),
]
schema = StructType().add("Users", StringType()).add("Books", StringType())
bookDataFrame = spark.createDataFrame(data,schema)
bookDataFrame.show()
# collect list
# bookDataFrame.groupBy('Users').agg(collect_list('Books')).show() #collect the comon books to each user
##pivot
dataFrame.groupBy('Payment_Method').pivot('Category').agg(avg('Price')).show()
#'Payment_Method' is the rows / 'category' is the columns / the 'Price' is the data
# +----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+
# |  Payment_Method|            Beauty|             Books|          Clothing|       Electronics|    Home & Kitchen|            Sports|              Toys|
# +----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+
# |     Credit Card|228.36680000000018|271.74247706422017|272.51357798165145| 264.6346666666667|236.98260504201684| 242.2463716814159|244.50885714285712|
# |     Net Banking|       269.2553125|265.11691588785044| 252.4574489795919|254.89800000000002|254.32964285714283|251.65087912087904| 239.4035714285715|
# |      Debit Card| 254.2083333333333|264.38609523809544|253.25102803738312|236.82080808080804| 263.5253846153845|272.91598290598296|258.63880733944956|
# |Cash on Delivery| 271.4009090909091|229.55203883495153|269.92486238532103|239.16695652173917|270.23208333333326|251.64075471698104| 267.5665934065933|
# |             UPI|243.33675000000014| 263.4533636363637| 266.2043518518519| 260.7360784313725|227.17576271186437|  275.299247311828| 237.5538679245284|
# +----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+
dataFrame.show()
#When-Otherwise
dataFrame.withColumn('flag', when(dataFrame['Category']=='Sports', 'non-veg').otherwise('veg')).show() # add a condition...
dataFrame.withColumn('flag', when((dataFrame['Category']=='Sports') & (dataFrame['Price'] <= 100),'good-price')\
        .when((dataFrame['Category']=='Sports') & (dataFrame['Price'] > 100),'expensive-price')\
        .otherwise('Non-sport')).show() # add multiple conditions...
# -------------------------------Joins---------------------------------------------

spark = SparkSession.builder.appName("Spark").getOrCreate()
#data transformation
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
#filtering the data
dataFrame.select('User_ID', 'Product_ID', 'Category').show() #selecting what to show
dataFrame.select(dataFrame['User_ID'].alias('Item_ID')).show() #alias change the name
dataFrame.filter(dataFrame['Category']=='Sports').show() #filtering the data scenario 1
dataFrame.filter((dataFrame['Price']<100) & (dataFrame['Category']=='Books')).show() #filtering the data scenario 2
dataFrame.filter((dataFrame['Price']<100) & (dataFrame['Category']=='Books') & (dataFrame['Payment_Method']=='UPI')).show() #filtering the data scenario 3
#withColumn
dataFrame.withColumnRenamed('User_ID','Item_ID').show() #changing the name of the column
dataFrame.withColumn('Name', lit("new")) #creating new column with name 'Name' with constant 'new'
dataFrame.withColumn("Category", lit("HAHA")).show() #changing the existing column "Category" with the constant "haha"
dataFrame.withColumn('Multiple', dataFrame['Price']*dataFrame['Discount']).show() # creating new column 'Multiple' and put in it the multiple of the column 'Price' with the column 'Discount'
dataFrame.withColumn('Category', regexp_replace('Category', "Sports", "spts")).show() #changing every value "Sports" in an existing column named "Category"
dataFrame.withColumn('Price', dataFrame['Price'].cast(IntegerType())).show() #cast the type of a column
#sort
dataFrame.sort('Price', ascending=False).show() #sort descending
dataFrame.sort('Final_Price').show() #sort ascending
dataFrame.sort('Price', 'Discount', ascending=[0,1]).show() #sort multiple columns
dataFrame.limit(10).show() #show only first 10 lines
#handle null data
dataFrame.drop('Final_Price').show() #remove a column
dataFrame.drop('Final_Price','Purchase_Date').show() #remove 2 columns
dataFrame.dropDuplicates().show() #remove the duplicates from all the columns
dataFrame.distinct().show() #same as dropDuplicate
dataFrame.dropDuplicates(subset=['Category']).show() #remove the duplicate from one column
#union 2 dataSets
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
#split and indexing
newDataFrame = dataFrame.filter(dataFrame['Payment_Method']=='Net Banking')
newDataFrame.show()
newDataFrameSplit = newDataFrame.withColumn('Payment_Method', split('Payment_Method', ' ')) #spliting the data 'Net Banking' into [Net, Banking]
newDataFrame.withColumn('Payment_Method', split('Payment_Method', ' ')[1]).show() #spliting the data 'Net Banking' into [Net, Banking] and put only the index one wish is 'Banking'
# explode
newDataFrameSplit.withColumn('Payment_Method', explode('Payment_Method')).show() #explode the [Net, Bnaking] in 2 rows 'Net' in a row and 'Banking' in a row
# array_contains (check in an array if somthing exist or not)
newDataFrameSplit.withColumn('Flag', array_contains('Payment_Method', 'Net')).show() #create a new column 'flag' and put int it true or false if the 'Net' is there or not
#***********************************[group by] the most popular function in data world ***************************************************************************
#senario 1 (1 column)
dataFrame.groupBy('Category').agg(sum('Price')).show() #the sum
dataFrame.groupBy('Category').agg(mean('Price')).show() #the mean
dataFrame.groupBy('Category').agg(avg('Price')).show() #the average
#senario 2 (multiple columns)
dataFrame.groupBy('Category', 'Product_ID').agg(sum('Price'), mean('Price'), sum('Discount'), mean('Discount')).show()
#advanced pyspark
data =[
        ('user1', 'book1'),
        ('user1', 'book2'),
        ('user2', 'book2'),
        ('user2', 'book4'),
        ('user3', 'book1'),
]
schema = StructType().add("Users", StringType()).add("Books", StringType())
bookDataFrame = spark.createDataFrame(data,schema)
bookDataFrame.show()
# collect list
# bookDataFrame.groupBy('Users').agg(collect_list('Books')).show() #collect the comon books to each user
##pivot
dataFrame.groupBy('Payment_Method').pivot('Category').agg(avg('Price')).show()
#'Payment_Method' is the rows / 'category' is the columns / the 'Price' is the data
# +----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+
# |  Payment_Method|            Beauty|             Books|          Clothing|       Electronics|    Home & Kitchen|            Sports|              Toys|
# +----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+
# |     Credit Card|228.36680000000018|271.74247706422017|272.51357798165145| 264.6346666666667|236.98260504201684| 242.2463716814159|244.50885714285712|
# |     Net Banking|       269.2553125|265.11691588785044| 252.4574489795919|254.89800000000002|254.32964285714283|251.65087912087904| 239.4035714285715|
# |      Debit Card| 254.2083333333333|264.38609523809544|253.25102803738312|236.82080808080804| 263.5253846153845|272.91598290598296|258.63880733944956|
# |Cash on Delivery| 271.4009090909091|229.55203883495153|269.92486238532103|239.16695652173917|270.23208333333326|251.64075471698104| 267.5665934065933|
# |             UPI|243.33675000000014| 263.4533636363637| 266.2043518518519| 260.7360784313725|227.17576271186437|  275.299247311828| 237.5538679245284|
# +----------------+------------------+------------------+------------------+------------------+------------------+------------------+------------------+
dataFrame.show()
#When-Otherwise
dataFrame.withColumn('flag', when(dataFrame['Category']=='Sports', 'non-veg').otherwise('veg')).show() # add a condition...
dataFrame.withColumn('flag', when((dataFrame['Category']=='Sports') & (dataFrame['Price'] <= 100),'good-price')\
        .when((dataFrame['Category']=='Sports') & (dataFrame['Price'] > 100),'expensive-price')\
        .otherwise('Non-sport')).show() # add multiple conditions...
# -------------------------------Joins---------------------------------------------
