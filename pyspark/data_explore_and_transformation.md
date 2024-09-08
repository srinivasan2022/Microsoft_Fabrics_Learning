### Data exploration and transformation
```
from pyspark.sql import SparkSession

file_path = "Files/data/ipl_summary_raw.csv"

# Read the CSV file
df = spark.read.csv(file_path , header=True , inferSchema = True)
```

###### count and groupby function
```
df.groupBy("info_city").count().show()
```
<pre>
+------------+-----+
|   info_city|count|
+------------+-----+
|   Bangalore|   65|
|       Kochi|    5|
|     Chennai|   85|
|     Lucknow|   14|
| Navi Mumbai|    9|
|        null|   51|
|   Centurion|   12|
|      Ranchi|    7|
|      Mumbai|  173|
|   Ahmedabad|   36|
|      Durban|   15|
|     Kolkata|   93|
|   Cape Town|    7|
|  Dharamsala|   13|
|     Sharjah|   10|
|        Pune|   51|
|Johannesburg|    8|
|   Kimberley|    3|
|       Delhi|   90|
|      Raipur|    6|
+------------+-----+
</pre>

```
df.groupBy("info_city" , "info_outcome_winner").count().show()
```
<pre>
+-------------+--------------------+-----+
|    info_city| info_outcome_winner|count|
+-------------+--------------------+-----+
|    Bangalore|                null|    3|
|        Dubai|    Rajasthan Royals|    1|
|      Chennai|     Kings XI Punjab|    1|
|       Kanpur|    Delhi Daredevils|    1|
|     Guwahati|        Punjab Kings|    2|
|       Indore|      Mumbai Indians|    2|
|      Chennai|      Delhi Capitals|    1|
|      Kolkata|Kochi Tuskers Kerala|    1|
|        Delhi|Kolkata Knight Ri...|    5|
|       Indore|Royal Challengers...|    1|
|         Pune|      Gujarat Titans|    3|
|    Hyderabad|      Delhi Capitals|    2|
|       Kanpur|       Gujarat Lions|    2|
|Visakhapatnam|    Rajasthan Royals|    1|
|    Ahmedabad|Royal Challengers...|    2|
|       Mumbai|      Gujarat Titans|    6|
|  Navi Mumbai|        Punjab Kings|    1|
|         null| Sunrisers Hyderabad|    8|
|    Abu Dhabi|Kolkata Knight Ri...|    7|
|        Dubai| Sunrisers Hyderabad|    1|
+-------------+--------------------+-----+
</pre>

```
df.groupBy("info_city" , "info_outcome_winner" , "info_toss_decision").count().show()
```
<pre>
+--------------+--------------------+------------------+-----+
|     info_city| info_outcome_winner|info_toss_decision|count|
+--------------+--------------------+------------------+-----+
|        Indore|     Kings XI Punjab|             field|    3|
|        Indore|Kochi Tuskers Kerala|             field|    1|
|         Dubai|        Punjab Kings|             field|    2|
|       Chennai| Sunrisers Hyderabad|             field|    1|
|       Kolkata|       Gujarat Lions|             field|    2|
|       Kolkata|Lucknow Super Giants|             field|    1|
|     Hyderabad|     Kings XI Punjab|             field|    4|
|        Mumbai|Rising Pune Super...|               bat|    1|
|    Chandigarh|     Kings XI Punjab|             field|   23|
|       Sharjah|Kolkata Knight Ri...|             field|    3|
|          Pune|      Gujarat Titans|               bat|    1|
|Port Elizabeth|     Kings XI Punjab|               bat|    1|
|    Chandigarh|       Pune Warriors|               bat|    1|
|     Bengaluru|      Mumbai Indians|             field|    1|
|          Pune|        Punjab Kings|             field|    1|
|     Centurion|Royal Challengers...|             field|    1|
|        Mumbai|     Deccan Chargers|               bat|    1|
|        Kanpur|    Delhi Daredevils|             field|    1|
|         Dubai|      Delhi Capitals|               bat|    1|
|     Ahmedabad|Royal Challengers...|             field|    1|
+--------------+--------------------+------------------+-----+
</pre>

###### Filter and Select

```
chennai_win = df.select("info_city" , "Info_outcome_winner").filter(df.info_city == "Chennai")
chennai_win.show()
```
<pre>
+---------+--------------------+
|info_city| Info_outcome_winner|
+---------+--------------------+
|  Chennai| Chennai Super Kings|
|  Chennai| Chennai Super Kings|
|  Chennai| Chennai Super Kings|
|  Chennai| Chennai Super Kings|
|  Chennai| Chennai Super Kings|
|  Chennai| Chennai Super Kings|
|  Chennai|      Mumbai Indians|
|  Chennai| Chennai Super Kings|
|  Chennai|      Mumbai Indians|
|  Chennai|Royal Challengers...|
|  Chennai|Kolkata Knight Ri...|
|  Chennai|      Mumbai Indians|
|  Chennai|Royal Challengers...|
|  Chennai|      Mumbai Indians|
|  Chennai|Royal Challengers...|
|  Chennai|      Delhi Capitals|
|  Chennai| Sunrisers Hyderabad|
|  Chennai|        Punjab Kings|
|  Chennai|                null|
|  Chennai| Chennai Super Kings|
+---------+--------------------+
</pre>

```
Mumbai_win_2017 = df.select("info_city" , "info_season" , "info_outcome_winner").filter((df.info_city == "Mumbai") & (df.info_season == 2017))
Mumbai_win_2017.show()
```
<pre>
+---------+-----------+--------------------+
|info_city|info_season| info_outcome_winner|
+---------+-----------+--------------------+
|   Mumbai|       2017|      Mumbai Indians|
|   Mumbai|       2017|      Mumbai Indians|
|   Mumbai|       2017|      Mumbai Indians|
|   Mumbai|       2017|      Mumbai Indians|
|   Mumbai|       2017|Rising Pune Super...|
|   Mumbai|       2017|      Mumbai Indians|
|   Mumbai|       2017|     Kings XI Punjab|
|   Mumbai|       2017|Rising Pune Super...|
+---------+-----------+--------------------+
</pre>

###### Renamed the column named

```
re_col = df.withColumnRenamed("info_outcome_winner" , "winning_team")
re_col.select("winning_team").show()
```
<pre>
+--------------------+
|        winning_team|
+--------------------+
| Sunrisers Hyderabad|
|Rising Pune Super...|
|Kolkata Knight Ri...|
|     Kings XI Punjab|
|Royal Challengers...|
| Sunrisers Hyderabad|
|      Mumbai Indians|
|     Kings XI Punjab|
|    Delhi Daredevils|
|      Mumbai Indians|
|Kolkata Knight Ri...|
|      Mumbai Indians|
|       Gujarat Lions|
|Kolkata Knight Ri...|
|    Delhi Daredevils|
|      Mumbai Indians|
|Rising Pune Super...|
|Kolkata Knight Ri...|
| Sunrisers Hyderabad|
|Royal Challengers...|
+--------------------+
</pre>

```
from pyspark.sql.functions import when, col

# Add 'win_by_type' column
df = df.withColumn("win_by_type", 
                   when(col("info_outcome_by_runs").isNotNull(), "runs")
                   .when(col("info_outcome_by_wickets").isNotNull(), "wickets")
                   .otherwise("unknown"))
df.select("win_by_type").show()
```
<pre>
+-----------+
|win_by_type|
+-----------+
|       runs|
|    wickets|
|    wickets|
|    wickets|
|       runs|
|    wickets|
|    wickets|
|    wickets|
|       runs|
|    wickets|
|    wickets|
|    wickets|
|    wickets|
|       runs|
|       runs|
|    wickets|
|       runs|
|    wickets|
|       runs|
|       runs|
+-----------+
</pre>

###### Isin or contain

```
players=["GJ Maxwell","B Kumar"]
players_teams = df.filter(df.info_player_of_match_1.isin(players)).select("info_season","info_player_of_match_1")
players_teams.show()
```
<pre>
+-----------+----------------------+
|info_season|info_player_of_match_1|
+-----------+----------------------+
|       2017|            GJ Maxwell|
|       2017|               B Kumar|
|       2021|            GJ Maxwell|
|       2021|            GJ Maxwell|
|       2021|            GJ Maxwell|
|       2023|            GJ Maxwell|
|       2024|               B Kumar|
|       2014|            GJ Maxwell|
|       2014|            GJ Maxwell|
|       2014|            GJ Maxwell|
|       2014|               B Kumar|
|       2014|            GJ Maxwell|
|       2014|               B Kumar|
|       2016|               B Kumar|
|       2016|               B Kumar|
+-----------+----------------------+
</pre>


```
maxwell_matches = df.filter(df.info_player_of_match_1.contains("V Kohli"))

maxwell_matches.select("info_season","info_player_of_match_1").show()
```
<pre>
+-----------+----------------------+
|info_season|info_player_of_match_1|
+-----------+----------------------+
|       2019|               V Kohli|
|    2020/21|               V Kohli|
|       2022|               V Kohli|
|       2023|               V Kohli|
|       2023|               V Kohli|
|       2024|               V Kohli|
|       2024|               V Kohli|
|       2011|               V Kohli|
|       2011|               V Kohli|
|       2013|               V Kohli|
|       2013|               V Kohli|
|       2013|               V Kohli|
|       2015|               V Kohli|
|       2016|               V Kohli|
|       2016|               V Kohli|
|       2016|               V Kohli|
|       2016|               V Kohli|
|       2016|               V Kohli|
+-----------+----------------------+
</pre>

###### Like Operator
```
sunrisers_matches = df.filter(df.info_teams_1.like("%Sunrisers%"))
sunrisers_matches.show(truncate=False)
```
<pre>
+-------------------+----------+------------+-----------------------+---------------------+-----------+-------------------+-------------------------------+--------------------------------+---------------------------+------------------------+------------------------+--------------------+----------------------+----------+----------------------+-----------+--------------+-------------------+---------------------------+------------------+---------------------------+--------------------------------------------+-----------------------+-----------------------+-------------------+----------------+------------+-------------------+
|info_balls_per_over|info_city |info_dates_1|info_event_match_number|info_event_name      |info_gender|info_match_type    |info_officials_match_referees_1|info_officials_reserve_umpires_1|info_officials_tv_umpires_1|info_officials_umpires_1|info_officials_umpires_2|info_outcome_by_runs|info_outcome_winner   |info_overs|info_player_of_match_1|info_season|info_team_type|info_teams_1       |info_teams_2               |info_toss_decision|info_toss_winner           |info_venue                                  |info_outcome_by_wickets|info_outcome_eliminator|info_outcome_result|info_event_stage|info_dates_2|info_outcome_method|
+-------------------+----------+------------+-----------------------+---------------------+-----------+-------------------+-------------------------------+--------------------------------+---------------------------+------------------------+------------------------+--------------------+----------------------+----------+----------------------+-----------+--------------+-------------------+---------------------------+------------------+---------------------------+--------------------------------------------+-----------------------+-----------------------+-------------------+----------------+------------+-------------------+
|6                  |Hyderabad |2017-04-05  |1.0                    |Indian Premier League|male       |2024-09-08 20:00:00|J Srinath                      |N Pandit                        |A Deshmukh                 |AY Dandekar             |NJ Llong                |35.0                |Sunrisers Hyderabad   |20        |Yuvraj Singh          |2017       |club          |Sunrisers Hyderabad|Royal Challengers Bangalore|field             |Royal Challengers Bangalore|Rajiv Gandhi International Stadium, Uppal   |null                   |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2017-04-09  |6.0                    |Indian Premier League|male       |2024-09-08 20:00:00|M Nayyar                       |N Pandit                        |AY Dandekar                |A Deshmukh              |NJ Llong                |null                |Sunrisers Hyderabad   |20        |Rashid Khan           |2017       |club          |Sunrisers Hyderabad|Gujarat Lions              |field             |Sunrisers Hyderabad        |Rajiv Gandhi International Stadium, Uppal   |9.0                    |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2017-04-17  |19.0                   |Indian Premier League|male       |2024-09-08 20:00:00|AJ Pycroft                     |N Pandit                        |NJ Llong                   |AY Dandekar             |A Deshmukh              |5.0                 |Sunrisers Hyderabad   |20        |B Kumar               |2017       |club          |Sunrisers Hyderabad|Kings XI Punjab            |field             |Kings XI Punjab            |Rajiv Gandhi International Stadium, Uppal   |null                   |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2017-04-19  |21.0                   |Indian Premier League|male       |2024-09-08 20:00:00|M Nayyar                       |N Pandit                        |AY Dandekar                |CB Gaffaney             |NJ Llong                |15.0                |Sunrisers Hyderabad   |20        |KS Williamson         |2017       |club          |Sunrisers Hyderabad|Delhi Daredevils           |bat               |Sunrisers Hyderabad        |Rajiv Gandhi International Stadium, Uppal   |null                   |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2017-04-30  |37.0                   |Indian Premier League|male       |2024-09-08 20:00:00|J Srinath                      |Navdeep Singh                   |VK Sharma                  |AY Dandekar             |S Ravi                  |48.0                |Sunrisers Hyderabad   |20        |DA Warner             |2017       |club          |Sunrisers Hyderabad|Kolkata Knight Riders      |field             |Kolkata Knight Riders      |Rajiv Gandhi International Stadium, Uppal   |null                   |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2017-05-06  |44.0                   |Indian Premier League|male       |2024-09-08 20:00:00|Chinmay Sharma                 |R Pandit                        |M Erasmus                  |KN Ananthapadmanabhan   |AK Chaudhary            |12.0                |Rising Pune Supergiant|20        |JD Unadkat            |2017       |club          |Sunrisers Hyderabad|Rising Pune Supergiant     |field             |Sunrisers Hyderabad        |Rajiv Gandhi International Stadium, Uppal   |null                   |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2017-05-08  |48.0                   |Indian Premier League|male       |2024-09-08 20:00:00|Chinmay Sharma                 |R Pandit                        |AK Chaudhary               |KN Ananthapadmanabhan   |M Erasmus               |null                |Sunrisers Hyderabad   |20        |S Dhawan              |2017       |club          |Sunrisers Hyderabad|Mumbai Indians             |bat               |Mumbai Indians             |Rajiv Gandhi International Stadium, Uppal   |7.0                    |null                   |null               |null            |null        |null               |
|6                  |Bangalore |2017-05-17  |null                   |Indian Premier League|male       |2024-09-08 20:00:00|M Nayyar                       |KN Ananthapadmanabhan           |A Nand Kishore             |AK Chaudhary            |Nitin Menon             |null                |Kolkata Knight Riders |20        |NM Coulter-Nile       |2017       |club          |Sunrisers Hyderabad|Kolkata Knight Riders      |field             |Kolkata Knight Riders      |M Chinnaswamy Stadium                       |7.0                    |null                   |null               |Eliminator      |2017-05-18  |D/L                |
|6                  |Mumbai    |2018-04-24  |23.0                   |Indian Premier League|male       |2024-09-08 20:00:00|V Narayan Kutty                |K Srinath                       |A Deshmukh                 |C Shamshuddin           |S Ravi                  |31.0                |Sunrisers Hyderabad   |20        |Rashid Khan           |2018       |club          |Sunrisers Hyderabad|Mumbai Indians             |field             |Mumbai Indians             |Wankhede Stadium                            |null                   |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2018-04-26  |25.0                   |Indian Premier League|male       |2024-09-08 20:00:00|J Srinath                      |GR Sadashiv Iyer                |RJ Tucker                  |CK Nandan               |YC Barde                |13.0                |Sunrisers Hyderabad   |20        |AS Rajpoot            |2018       |club          |Sunrisers Hyderabad|Kings XI Punjab            |field             |Kings XI Punjab            |Rajiv Gandhi International Stadium          |null                   |null                   |null               |null            |null        |null               |
|6                  |Jaipur    |2018-04-29  |28.0                   |Indian Premier League|male       |2024-09-08 20:00:00|AJ Pycroft                     |GR Sadashiv Iyer                |S Ravi                     |BNJ Oxenford            |A Nand Kishore          |11.0                |Sunrisers Hyderabad   |20        |KS Williamson         |2018       |club          |Sunrisers Hyderabad|Rajasthan Royals           |bat               |Sunrisers Hyderabad        |Sawai Mansingh Stadium                      |null                   |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2018-05-07  |39.0                   |Indian Premier League|male       |2024-09-08 20:00:00|AJ Pycroft                     |GR Sadashiv Iyer                |CK Nandan                  |BNJ Oxenford            |VK Sharma               |5.0                 |Sunrisers Hyderabad   |20        |KS Williamson         |2018       |club          |Sunrisers Hyderabad|Royal Challengers Bangalore|field             |Royal Challengers Bangalore|Rajiv Gandhi International Stadium          |null                   |null                   |null               |null            |null        |null               |
|6                  |Pune      |2018-05-13  |46.0                   |Indian Premier League|male       |2024-09-08 20:00:00|M Nayyar                       |HAS Khalid                      |AY Dandekar                |M Erasmus               |YC Barde                |null                |Chennai Super Kings   |20        |AT Rayudu             |2018       |club          |Sunrisers Hyderabad|Chennai Super Kings        |field             |Chennai Super Kings        |Maharashtra Cricket Association Stadium     |8.0                    |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2018-05-19  |54.0                   |Indian Premier League|male       |2024-09-08 20:00:00|Prakash Bhatt                  |HAS Khalid                      |AY Dandekar                |AK Chaudhary            |S Ravi                  |null                |Kolkata Knight Riders |20        |CA Lynn               |2018       |club          |Sunrisers Hyderabad|Kolkata Knight Riders      |bat               |Sunrisers Hyderabad        |Rajiv Gandhi International Stadium          |5.0                    |null                   |null               |null            |null        |null               |
|6                  |Mumbai    |2018-05-22  |null                   |Indian Premier League|male       |2024-09-08 20:00:00|AJ Pycroft                     |YC Barde                        |S Ravi                     |C Shamshuddin           |M Erasmus               |null                |Chennai Super Kings   |20        |F du Plessis          |2018       |club          |Sunrisers Hyderabad|Chennai Super Kings        |field             |Chennai Super Kings        |Wankhede Stadium                            |2.0                    |null                   |null               |Qualifier 1     |null        |null               |
|6                  |Kolkata   |2018-05-25  |null                   |Indian Premier League|male       |2024-09-08 20:00:00|J Srinath                      |A Nand Kishore                  |AK Chaudhary               |HDPK Dharmasena         |Nitin Menon             |14.0                |Sunrisers Hyderabad   |20        |Rashid Khan           |2018       |club          |Sunrisers Hyderabad|Kolkata Knight Riders      |field             |Kolkata Knight Riders      |Eden Gardens                                |null                   |null                   |null               |Qualifier 2     |null        |null               |
|6                  |Mumbai    |2018-05-27  |null                   |Indian Premier League|male       |2024-09-08 20:00:00|AJ Pycroft                     |YC Barde                        |Nitin Menon                |M Erasmus               |S Ravi                  |null                |Chennai Super Kings   |20        |SR Watson             |2018       |club          |Sunrisers Hyderabad|Chennai Super Kings        |field             |Chennai Super Kings        |Wankhede Stadium                            |8.0                    |null                   |null               |Final           |null        |null               |
|6                  |Kolkata   |2019-03-24  |2.0                    |Indian Premier League|male       |2024-09-08 20:00:00|V Narayan Kutty                |GR Sadashiv Iyer                |VA Kulkarni                |AK Chaudhary            |CB Gaffaney             |null                |Kolkata Knight Riders |20        |AD Russell            |2019       |club          |Sunrisers Hyderabad|Kolkata Knight Riders      |field             |Kolkata Knight Riders      |Eden Gardens                                |6.0                    |null                   |null               |null            |null        |null               |
|6                  |Hyderabad |2019-03-31  |11.0                   |Indian Premier League|male       |2024-09-08 20:00:00|Chinmay Sharma                 |K Srinath                       |C Shamshuddin              |KN Ananthapadmanabhan   |S Ravi                  |118.0               |Sunrisers Hyderabad   |20        |JM Bairstow           |2019       |club          |Sunrisers Hyderabad|Royal Challengers Bangalore|field             |Royal Challengers Bangalore|Rajiv Gandhi International Stadium          |null                   |null                   |null               |null            |null        |null               |
|6                  |Chandigarh|2019-04-08  |22.0                   |Indian Premier League|male       |2024-09-08 20:00:00|J Srinath                      |HAS Khalid                      |Nitin Menon                |AY Dandekar             |M Erasmus               |null                |Kings XI Punjab       |20        |KL Rahul              |2019       |club          |Sunrisers Hyderabad|Kings XI Punjab            |field             |Kings XI Punjab            |Punjab Cricket Association IS Bindra Stadium|6.0                    |null                   |null               |null            |null        |null               |
+-------------------+----------+------------+-----------------------+---------------------+-----------+-------------------+-------------------------------+--------------------------------+---------------------------+------------------------+------------------------+--------------------+----------------------+----------+----------------------+-----------+--------------+-------------------+---------------------------+------------------+---------------------------+--------------------------------------------+-----------------------+-----------------------+-------------------+----------------+------------+-------------------+
</pre>

###### where for filtering
```
chennai_matches = df.where((df.info_city == "Chennai") & (df.info_season == 2024)).select("info_teams_1","info_teams_2")

chennai_matches.show(truncate=False)
```
<pre>
+---------------------------+---------------------+
|info_teams_1               |info_teams_2         |
+---------------------------+---------------------+
|Royal Challengers Bengaluru|Chennai Super Kings  |
|Chennai Super Kings        |Gujarat Titans       |
|Kolkata Knight Riders      |Chennai Super Kings  |
|Chennai Super Kings        |Lucknow Super Giants |
|Chennai Super Kings        |Sunrisers Hyderabad  |
|Chennai Super Kings        |Punjab Kings         |
|Rajasthan Royals           |Chennai Super Kings  |
|Sunrisers Hyderabad        |Rajasthan Royals     |
|Sunrisers Hyderabad        |Kolkata Knight Riders|
+---------------------------+---------------------+
</pre>

###### sort or orderBy
```
sorted_matches = df.sort(df.info_outcome_by_runs.desc()).select("info_teams_1","info_teams_2","info_outcome_by_runs","info_outcome_winner")

sorted_matches.show(truncate=False)
```
<pre>
+---------------------------+---------------------------+--------------------+---------------------------+
|info_teams_1               |info_teams_2               |info_outcome_by_runs|info_outcome_winner        |
+---------------------------+---------------------------+--------------------+---------------------------+
|Delhi Daredevils           |Mumbai Indians             |146.0               |Mumbai Indians             |
|Royal Challengers Bangalore|Gujarat Lions              |144.0               |Royal Challengers Bangalore|
|Royal Challengers Bangalore|Kolkata Knight Riders      |140.0               |Kolkata Knight Riders      |
|Royal Challengers Bangalore|Kings XI Punjab            |138.0               |Royal Challengers Bangalore|
|Royal Challengers Bangalore|Pune Warriors              |130.0               |Royal Challengers Bangalore|
|Sunrisers Hyderabad        |Royal Challengers Bangalore|118.0               |Sunrisers Hyderabad        |
|Royal Challengers Bangalore|Rajasthan Royals           |112.0               |Royal Challengers Bangalore|
|Kings XI Punjab            |Royal Challengers Bangalore|111.0               |Kings XI Punjab            |
|Kolkata Knight Riders      |Delhi Capitals             |106.0               |Kolkata Knight Riders      |
|Delhi Daredevils           |Rajasthan Royals           |105.0               |Rajasthan Royals           |
|Mumbai Indians             |Kolkata Knight Riders      |102.0               |Mumbai Indians             |
|Delhi Daredevils           |Mumbai Indians             |98.0                |Mumbai Indians             |
|Kolkata Knight Riders      |Lucknow Super Giants       |98.0                |Kolkata Knight Riders      |
|Kings XI Punjab            |Royal Challengers Bangalore|97.0                |Kings XI Punjab            |
|Rising Pune Supergiant     |Delhi Daredevils           |97.0                |Delhi Daredevils           |
|Chennai Super Kings        |Kings XI Punjab            |97.0                |Chennai Super Kings        |
|Chennai Super Kings        |Delhi Daredevils           |93.0                |Chennai Super Kings        |
|Royal Challengers Bangalore|Chennai Super Kings        |92.0                |Chennai Super Kings        |
|Kolkata Knight Riders      |Mumbai Indians             |92.0                |Mumbai Indians             |
|Chennai Super Kings        |Delhi Capitals             |91.0                |Chennai Super Kings        |
+---------------------------+---------------------------+--------------------+---------------------------+
</pre>

###### Using SQL queries on dataframe
```
df.createOrReplaceTempView("ipl_matches")

# Use SQL query to find matches where "B Kumar" played
b_kumar_matches = spark.sql("SELECT * FROM ipl_matches WHERE info_player_of_match_1 LIKE '%B Kumar%'").select("info_teams_1","info_teams_2","info_outcome_by_runs","info_outcome_winner")

b_kumar_matches.show(truncate=False)
```
<pre>
+-------------------+-------------------+--------------------+-------------------+
|info_teams_1       |info_teams_2       |info_outcome_by_runs|info_outcome_winner|
+-------------------+-------------------+--------------------+-------------------+
|Sunrisers Hyderabad|Kings XI Punjab    |5.0                 |Sunrisers Hyderabad|
|Sunrisers Hyderabad|Rajasthan Royals   |1.0                 |Sunrisers Hyderabad|
|Mumbai Indians     |Sunrisers Hyderabad|15.0                |Sunrisers Hyderabad|
|Rajasthan Royals   |Sunrisers Hyderabad|32.0                |Sunrisers Hyderabad|
|Gujarat Lions      |Sunrisers Hyderabad|null                |Sunrisers Hyderabad|
|Sunrisers Hyderabad|Gujarat Lions      |null                |Sunrisers Hyderabad|
+-------------------+-------------------+--------------------+-------------------+
</pre>