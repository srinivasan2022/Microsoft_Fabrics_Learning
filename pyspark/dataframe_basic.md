### Creates the Dataframe :


```
data = [("United States","US",40,44,42,126),
("China","CHN",40,27,24,91),
("Japan","JPN",20,12,13,45),
("Australia","AUS",18,19,16,53),
("France","FRA",16,26,22,64),
("Netherlands","NED",15,7,12,34),
("Great Britain","GBG",14,22,29,65),
("South Korea","KOR",13,9,10,32),
("Italy","ITA",12,13,15,40),
("Germany","GER",12,13,8,33),
("New Zealand","NZ",10,7,3,20)
]

columns = ["Country","Code","Gold","Sliver","Bronze","Total"]

df = spark.createDataFrame(data=data , schema=columns)

```
### Display the type of dataframe

```
df.printSchema()
```
<pre>
root
 |-- Country: string (nullable = true)
 |-- Code: string (nullable = true)
 |-- Gold: long (nullable = true)
 |-- Sliver: long (nullable = true)
 |-- Bronze: long (nullable = true)
 |-- Total: long (nullable = true)
 </pre>

### Display the dataframe

 ```
 df.show()
 ```
 <pre>
 +-------------+----+----+------+------+-----+
|Country      |Code|Gold|Sliver|Bronze|Total|
+-------------+----+----+------+------+-----+
|United States|US  |40  |44    |42    |126  |
|China        |CHN |40  |27    |24    |91   |
|Japan        |JPN |20  |12    |13    |45   |
|Australia    |AUS |18  |19    |16    |53   |
|France       |FRA |16  |26    |22    |64   |
|Netherlands  |NED |15  |7     |12    |34   |
|Great Britain|GBG |14  |22    |29    |65   |
|South Korea  |KOR |13  |9     |10    |32   |
|Italy        |ITA |12  |13    |15    |40   |
|Germany      |GER |12  |13    |8     |33   |
|New Zealand  |NZ  |10  |7     |3     |20   |
+-------------+----+----+------+------+-----+
 </pre>

 ### Type
 ```
 type (df)
 ```
 <p> pyspark.sql.dataframe.DataFrame </p>

 ```
 type(df.Country) </p>
 ```

 <p> pyspark.sql.column.Column </p>

 ```
type(df.Total)
 ```
 <p>pyspark.sql.column.Column  </p>

 ### Creates the dataframe using structType (python)
```
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("United States","US",40,44,42,126),
("China","CHN",40,27,24,91),
("Japan","JPN",20,12,13,45),
("Australia","AUS",18,19,16,53),
("France","FRA",16,26,22,64),
("Netherlands","NED",15,7,12,34),
("Great Britain","GBG",14,22,29,65),
("South Korea","KOR",13,9,10,32),
("Italy","ITA",12,13,15,40),
("Germany","GER",12,13,8,33),
("New Zealand","NZ",10,7,3,20)
]

columns = StructType([
    StructField("Country" , StringType() , True) ,
    StructField("Code" , StringType() , True) ,
    StructField("Gold" , IntegerType() , True) ,
    StructField("Silver" , IntegerType() , True) ,
    StructField("Bronze" , IntegerType() , True) ,
    StructField("Total" , IntegerType() , True)
])

dfpy = spark.createDataFrame(data=data , schema=columns)
dfpy.printSchema()
dfpy.show(truncate=False)
```
<pre>
root
 |-- Country: string (nullable = true)
 |-- Code: string (nullable = true)
 |-- Gold: integer (nullable = true)
 |-- Silver: integer (nullable = true)
 |-- Bronze: integer (nullable = true)
 |-- Total: integer (nullable = true)

+-------------+----+----+------+------+-----+
|Country      |Code|Gold|Silver|Bronze|Total|
+-------------+----+----+------+------+-----+
|United States|US  |40  |44    |42    |126  |
|China        |CHN |40  |27    |24    |91   |
|Japan        |JPN |20  |12    |13    |45   |
|Australia    |AUS |18  |19    |16    |53   |
|France       |FRA |16  |26    |22    |64   |
|Netherlands  |NED |15  |7     |12    |34   |
|Great Britain|GBG |14  |22    |29    |65   |
|South Korea  |KOR |13  |9     |10    |32   |
|Italy        |ITA |12  |13    |15    |40   |
|Germany      |GER |12  |13    |8     |33   |
|New Zealand  |NZ  |10  |7     |3     |20   |
+-------------+----+----+------+------+-----+
</pre>

### Creates the dataframe using Data Definition Language (ddl - Sql)

```
columns_ddl = "Country STRING, Code STRING, Gold INT,Sliver INT,Bronze INT, Total INT"
ddl = spark.createDataFrame(data=data,schema=columns_ddl)
ddl.printSchema()
ddl.show(truncate=False)
display(ddl)
```
<pre>
root
 |-- Country: string (nullable = true)
 |-- Code: string (nullable = true)
 |-- Gold: integer (nullable = true)
 |-- Sliver: integer (nullable = true)
 |-- Bronze: integer (nullable = true)
 |-- Total: integer (nullable = true)

+-------------+----+----+------+------+-----+
|Country      |Code|Gold|Sliver|Bronze|Total|
+-------------+----+----+------+------+-----+
|United States|US  |40  |44    |42    |126  |
|China        |CHN |40  |27    |24    |91   |
|Japan        |JPN |20  |12    |13    |45   |
|Australia    |AUS |18  |19    |16    |53   |
|France       |FRA |16  |26    |22    |64   |
|Netherlands  |NED |15  |7     |12    |34   |
|Great Britain|GBG |14  |22    |29    |65   |
|South Korea  |KOR |13  |9     |10    |32   |
|Italy        |ITA |12  |13    |15    |40   |
|Germany      |GER |12  |13    |8     |33   |
|New Zealand  |NZ  |10  |7     |3     |20   |
+-------------+----+----+------+------+-----+
</pre>

```
df.show(2)
```
<pre>
+-------------+----+----+------+------+-----+
|      Country|Code|Gold|Sliver|Bronze|Total|
+-------------+----+----+------+------+-----+
|United States|  US|  40|    44|    42|  126|
|        China| CHN|  40|    27|    24|   91|
+-------------+----+----+------+------+-----+
</pre>

> display(df.head(2)) --> display the first two rows <br>
> display(df.tail(2)) --> display the last two rows
 