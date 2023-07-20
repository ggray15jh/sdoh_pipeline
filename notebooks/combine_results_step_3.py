# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 3 - Combine the results
# MAGIC This step combines the results from steps 2a and 2b
# MAGIC <br>
# MAGIC ##### Input
# MAGIC This notebook assumes that the notes have already processing by step 2b and used the sdoh_name+'_free_text_flag' and sdoh_name+'_survey_flag'
# MAGIC <br>
# MAGIC ##### Output
# MAGIC This notebook produces a new SQL table breaking with the added sdoh_name+'_flag' column

# COMMAND ----------

# MAGIC %run "./Function_Library"

# COMMAND ----------

# MAGIC %run "./Parameters"

# COMMAND ----------

sdoh_name = sdoh_name_param
query_string = query_string_3 

# COMMAND ----------

notes_df = spark.sql(query_string)
notes_df.persist()
notes_df.show()

# COMMAND ----------

results_df = notes_df.withColumn(sdoh_name+'_flag',
                    F.when(F.col(sdoh_name+'_free_text_flag') > 0,1)
                    .when(F.col(sdoh_name+'_survey_flag') > 0,1)
                    .otherwise(0))

# COMMAND ----------

results_df.write.mode('overwrite').option('header',True).option("mergeSchema", "true").saveAsTable('keyword_matched_test')