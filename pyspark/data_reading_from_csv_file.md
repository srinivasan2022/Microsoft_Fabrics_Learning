### Read the data from CSV file

Let's upload the IPL cricket CSV file

```
from pyspark.sql import SparkSession

file_path = "Files/data/ipl_summary_raw.csv"

df = spark.read.csv(file_path , header=True , inferSchema=True)

df.show(5)
```
<pre>
+-------------------+---------+------------+-----------------------+--------------------+-----------+-------------------+-------------------------------+--------------------------------+---------------------------+------------------------+------------------------+--------------------+--------------------+----------+----------------------+-----------+--------------+--------------------+--------------------+------------------+--------------------+--------------------+-----------------------+-----------------------+-------------------+----------------+------------+-------------------+
|info_balls_per_over|info_city|info_dates_1|info_event_match_number|     info_event_name|info_gender|    info_match_type|info_officials_match_referees_1|info_officials_reserve_umpires_1|info_officials_tv_umpires_1|info_officials_umpires_1|info_officials_umpires_2|info_outcome_by_runs| info_outcome_winner|info_overs|info_player_of_match_1|info_season|info_team_type|        info_teams_1|        info_teams_2|info_toss_decision|    info_toss_winner|          info_venue|info_outcome_by_wickets|info_outcome_eliminator|info_outcome_result|info_event_stage|info_dates_2|info_outcome_method|
+-------------------+---------+------------+-----------------------+--------------------+-----------+-------------------+-------------------------------+--------------------------------+---------------------------+------------------------+------------------------+--------------------+--------------------+----------+----------------------+-----------+--------------+--------------------+--------------------+------------------+--------------------+--------------------+-----------------------+-----------------------+-------------------+----------------+------------+-------------------+
|                  6|Hyderabad|  2017-04-05|                    1.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                      J Srinath|                        N Pandit|                 A Deshmukh|             AY Dandekar|                NJ Llong|                35.0| Sunrisers Hyderabad|        20|          Yuvraj Singh|       2017|          club| Sunrisers Hyderabad|Royal Challengers...|             field|Royal Challengers...|Rajiv Gandhi Inte...|                   null|                   null|               null|            null|        null|               null|
|                  6|     Pune|  2017-04-06|                    2.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                       M Nayyar|                   Navdeep Singh|                  VK Sharma|          A Nand Kishore|                  S Ravi|                null|Rising Pune Super...|        20|             SPD Smith|       2017|          club|Rising Pune Super...|      Mumbai Indians|             field|Rising Pune Super...|Maharashtra Crick...|                    7.0|                   null|               null|            null|        null|               null|
|                  6|   Rajkot|  2017-04-07|                    3.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                V Narayan Kutty|                    K Srinivasan|                   YC Barde|             Nitin Menon|               CK Nandan|                null|Kolkata Knight Ri...|        20|               CA Lynn|       2017|          club|       Gujarat Lions|Kolkata Knight Ri...|             field|Kolkata Knight Ri...|Saurashtra Cricke...|                   10.0|                   null|               null|            null|        null|               null|
|                  6|   Indore|  2017-04-08|                    4.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                 Chinmay Sharma|                        R Pandit|       KN Ananthapadmana...|            AK Chaudhary|           C Shamshuddin|                null|     Kings XI Punjab|        20|            GJ Maxwell|       2017|          club|     Kings XI Punjab|Rising Pune Super...|             field|     Kings XI Punjab|Holkar Cricket St...|                    6.0|                   null|               null|            null|        null|               null|
|                  6|Bengaluru|  2017-04-08|                    5.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                      J Srinath|                   Navdeep Singh|             A Nand Kishore|                  S Ravi|               VK Sharma|                15.0|Royal Challengers...|        20|             KM Jadhav|       2017|          club|Royal Challengers...|    Delhi Daredevils|               bat|Royal Challengers...|M.Chinnaswamy Sta...|                   null|                   null|               null|            null|        null|               null|
+-------------------+---------+------------+-----------------------+--------------------+-----------+-------------------+-------------------------------+--------------------------------+---------------------------+------------------------+------------------------+--------------------+--------------------+----------+----------------------+-----------+--------------+--------------------+--------------------+------------------+--------------------+--------------------+-----------------------+-----------------------+-------------------+----------------+------------+-------------------+
</pre>

```
df.printSchema()
```
<pre>
root
 |-- info_balls_per_over: integer (nullable = true)
 |-- info_city: string (nullable = true)
 |-- info_dates_1: date (nullable = true)
 |-- info_event_match_number: double (nullable = true)
 |-- info_event_name: string (nullable = true)
 |-- info_gender: string (nullable = true)
 |-- info_match_type: timestamp (nullable = true)
 |-- info_officials_match_referees_1: string (nullable = true)
 |-- info_officials_reserve_umpires_1: string (nullable = true)
 |-- info_officials_tv_umpires_1: string (nullable = true)
 |-- info_officials_umpires_1: string (nullable = true)
 |-- info_officials_umpires_2: string (nullable = true)
 |-- info_outcome_by_runs: double (nullable = true)
 |-- info_outcome_winner: string (nullable = true)
 |-- info_overs: integer (nullable = true)
 |-- info_player_of_match_1: string (nullable = true)
 |-- info_season: string (nullable = true)
 |-- info_team_type: string (nullable = true)
 |-- info_teams_1: string (nullable = true)
 |-- info_teams_2: string (nullable = true)
 |-- info_toss_decision: string (nullable = true)
 |-- info_toss_winner: string (nullable = true)
 |-- info_venue: string (nullable = true)
 |-- info_outcome_by_wickets: double (nullable = true)
 |-- info_outcome_eliminator: string (nullable = true)
 |-- info_outcome_result: string (nullable = true)
 |-- info_event_stage: string (nullable = true)
 |-- info_dates_2: date (nullable = true)
 |-- info_outcome_method: string (nullable = true)
</pre>

### Basic Operation

###### Select the specific columns
```
df.select("info_city" , "info_teams_1" , "info_teams_2").show()
```
<pre>
+---------+--------------------+--------------------+
|info_city|        info_teams_1|        info_teams_2|
+---------+--------------------+--------------------+
|Hyderabad| Sunrisers Hyderabad|Royal Challengers...|
|     Pune|Rising Pune Super...|      Mumbai Indians|
|   Rajkot|       Gujarat Lions|Kolkata Knight Ri...|
|   Indore|     Kings XI Punjab|Rising Pune Super...|
|Bengaluru|Royal Challengers...|    Delhi Daredevils|
|Hyderabad| Sunrisers Hyderabad|       Gujarat Lions|
|   Mumbai|      Mumbai Indians|Kolkata Knight Ri...|
|   Indore|     Kings XI Punjab|Royal Challengers...|
|     Pune|Rising Pune Super...|    Delhi Daredevils|
|   Mumbai|      Mumbai Indians| Sunrisers Hyderabad|
|  Kolkata|Kolkata Knight Ri...|     Kings XI Punjab|
|Bangalore|Royal Challengers...|      Mumbai Indians|
|   Rajkot|       Gujarat Lions|Rising Pune Super...|
|  Kolkata|Kolkata Knight Ri...| Sunrisers Hyderabad|
|    Delhi|    Delhi Daredevils|     Kings XI Punjab|
|   Mumbai|      Mumbai Indians|       Gujarat Lions|
|Bangalore|Royal Challengers...|Rising Pune Super...|
|    Delhi|    Delhi Daredevils|Kolkata Knight Ri...|
|Hyderabad| Sunrisers Hyderabad|     Kings XI Punjab|
|   Rajkot|       Gujarat Lions|Royal Challengers...|
+---------+--------------------+--------------------+,
</pre>

###### Show distinct values of a column
```
df.select("info_city").distinct().show()
```
<pre>
+------------+
|   info_city|
+------------+
|   Bangalore|
|       Kochi|
|     Chennai|
|     Lucknow|
| Navi Mumbai|
|        null|
|   Centurion|
|      Ranchi|
|      Mumbai|
|   Ahmedabad|
|      Durban|
|     Kolkata|
|   Cape Town|
|  Dharamsala|
|     Sharjah|
|        Pune|
|Johannesburg|
|   Kimberley|
|       Delhi|
|      Raipur|
+------------+
</pre>

```
df.select("info_teams_1").distinct().show()
```
<pre>
+--------------------+
|        info_teams_1|
+--------------------+
| Sunrisers Hyderabad|
|Lucknow Super Giants|
| Chennai Super Kings|
|      Gujarat Titans|
|Royal Challengers...|
|Rising Pune Super...|
|     Deccan Chargers|
|Kochi Tuskers Kerala|
|    Rajasthan Royals|
|       Gujarat Lions|
|Royal Challengers...|
|Kolkata Knight Ri...|
|Rising Pune Super...|
|     Kings XI Punjab|
|        Punjab Kings|
|       Pune Warriors|
|    Delhi Daredevils|
|      Delhi Capitals|
|      Mumbai Indians|
+--------------------+
</pre>

###### Filter rows based on a condition
```
df.filter(df["info_city"] == "Chennai").show()
```
<pre>
field|Royal Challengers...|MA Chidambaram St...|                    2.0|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-11|                    3.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                V Narayan Kutty|                      HAS Khalid|                CB Gaffaney|    KN Ananthapadmana...|             Nitin Menon|                10.0|Kolkata Knight Ri...|        20|                N Rana|       2021|          club|Kolkata Knight Ri...| Sunrisers Hyderabad|             field| Sunrisers Hyderabad|MA Chidambaram St...|                   null|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-13|                    5.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                   S Chaturvedi|                      HAS Khalid|       KN Ananthapadmana...|           C Shamshuddin|             CB Gaffaney|                10.0|      Mumbai Indians|        20|             RD Chahar|       2021|          club|      Mumbai Indians|Kolkata Knight Ri...|             field|Kolkata Knight Ri...|MA Chidambaram St...|                   null|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-14|                    6.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                V Narayan Kutty|                    Tapan Sharma|              C Shamshuddin|             Nitin Menon|               UV Gandhe|                 6.0|Royal Challengers...|        20|            GJ Maxwell|       2021|          club|Royal Challengers...| Sunrisers Hyderabad|             field| Sunrisers Hyderabad|MA Chidambaram St...|                   null|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-17|                    9.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                   S Chaturvedi|                    Tapan Sharma|       KN Ananthapadmana...|             CB Gaffaney|            K Srinivasan|                13.0|      Mumbai Indians|        20|            KA Pollard|       2021|          club|      Mumbai Indians| Sunrisers Hyderabad|               bat|      Mumbai Indians|MA Chidambaram St...|                   null|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-18|                   10.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                V Narayan Kutty|                    K Srinivasan|                CB Gaffaney|           C Shamshuddin|             Nitin Menon|                38.0|Royal Challengers...|        20|        AB de Villiers|       2021|          club|Royal Challengers...|Kolkata Knight Ri...|               bat|Royal Challengers...|MA Chidambaram St...|                   null|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-20|                   13.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                V Narayan Kutty|                    Tapan Sharma|                  UV Gandhe|           C Shamshuddin|             CB Gaffaney|                null|      Delhi Capitals|        20|              A Mishra|       2021|          club|      Mumbai Indians|      Delhi Capitals|               bat|      Mumbai Indians|MA Chidambaram St...|                    6.0|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-21|                   14.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                   S Chaturvedi|                      HAS Khalid|       KN Ananthapadmana...|            K Srinivasan|             Nitin Menon|                null| Sunrisers Hyderabad|        20|           JM Bairstow|       2021|          club|        Punjab Kings| Sunrisers Hyderabad|               bat|        Punjab Kings|MA Chidambaram St...|                    9.0|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-23|                   17.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                V Narayan Kutty|                       UV Gandhe|                 HAS Khalid|           C Shamshuddin|             Nitin Menon|                null|        Punjab Kings|        20|              KL Rahul|       2021|          club|      Mumbai Indians|        Punjab Kings|             field|        Punjab Kings|MA Chidambaram St...|                    9.0|                   null|               null|            null|        null|               null|
|                  6|  Chennai|  2021-04-25|                   20.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                   S Chaturvedi|                    K Srinivasan|               Tapan Sharma|             CB Gaffaney|    KN Ananthapadmana...|                null|                null|        20|               PP Shaw|       2021|          club|      Delhi Capitals| Sunrisers Hyderabad|               bat|      Delhi Capitals|MA Chidambaram St...|                   null|         Delhi Capitals|                tie|            null|        null|               null|
|                  6|  Chennai|  2023-04-03|                    6.0|Indian Premier Le...|       male|2024-08-31 20:00:00|                       M Nayyar|                        PM Joshi|                  UV Gandhe|                 A Totre|            BNJ Oxenford|                12.0| Chennai Super Kings|        20|                MM Ali|       2023|          club| Chennai Super Kings|Lucknow Super Giants|             field|Lucknow Super Giants|MA Chidambaram St...|                   null|                   null|               null|            null|        null|               null|
+-------------------+---------+------------+-----------------------+--------------------+-----------+-------------------+-------------------------------+--------------------------------+---------------------------+------------------------+------------------------+--------------------+--------------------+----------+----------------------+-----------+--------------+--------------------+--------------------+------------------+--------------------+--------------------+-----------------------+-----------------------+-------------------+----------------+------------+
</pre>

