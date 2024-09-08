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
### 

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