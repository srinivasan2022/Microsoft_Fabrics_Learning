### Data partitioning

###### Creates and save the file in lakehouse

```
# Partition the data by the 'info_season' column
df_partitioned = df.repartition("info_season")

# Write the partitioned Files to Folder (example: saving as Parquet format)
df_partitioned.write.parquet("Files/data/ipl_partitioned")
```
###### Creates and save the file in table format

```
# Partition the data by the 'info_season' column
df_partitioned = df.repartition("info_season")

# Write the partitioned Files to Folder (example: saving as Managed Table)
df_partitioned.write.saveAsTable("ipl_partitioned_table")

```
###### Creates and save the file in table format (Delta)

```
df_partitioned = df.repartition("info_season")

# Write the partitioned Files to Folder (example: saving as Delta Table)
df_partitioned.write.format("delta").saveAsTable("ipl_partitioned_delta_table")
```