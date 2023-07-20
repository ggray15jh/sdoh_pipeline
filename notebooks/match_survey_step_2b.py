# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 2b - Processing the semi-structured responses
# MAGIC This step produces a flag for patients with social determinants of health identified from the social narrative. 
# MAGIC <br>
# MAGIC ##### Input
# MAGIC This notebook assumes that the notes have already processing by step 2a and used the normalized_survey_text column
# MAGIC <br>
# MAGIC ##### Output
# MAGIC This notebook produces a new SQL table breaking with the sdoh_name+'_survey_flag' added

# COMMAND ----------

# MAGIC %run "./Function_Library"

# COMMAND ----------

# MAGIC %run "./Parameters"

# COMMAND ----------

query_string = query_string_2b
rules = rules_2b
sdoh_name = sdoh_name_param
input_col = input_col_2b 

# COMMAND ----------

notes_df = spark.sql(query_string)
notes_df.persist()
notes_df.show()

# COMMAND ----------

rulesPath = '/dbfs/tmp/rules.txt'

dbutils.fs.ls('dbfs:/tmp/')
dbutils.fs.put(rulesPath, rules, True)
dbutils.fs.ls('dbfs:/tmp/')

# COMMAND ----------

documentAssembler = DocumentAssembler().setInputCol(input_col).setOutputCol("document")

sentence = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")

regexMatcher = RegexMatcher() \
    .setExternalRules("dbfs:" + rulesPath, ",") \
    .setInputCols(["sentence"]) \
    .setOutputCol("regex") \
    .setStrategy("MATCH_ALL")

finisher = Finisher() \
    .setInputCols(['regex']) \
    .setOutputCols('result') \
    .setCleanAnnotations(True)
    
pipeline = Pipeline().setStages([documentAssembler, sentence, regexMatcher, finisher])

results_df = pipeline.fit(notes_df).transform(notes_df)

results_df = results_df.withColumn(sdoh_name+'_survey_flag',
                                   F.when(F.col('result').getItem(1) == 'yes',1)
                                   .when(F.col('result').getItem(1) == 'often true',1)
                                   .when(F.col('result').getItem(1) == 'sometimes true',1)
                                   .otherwise(0))

# COMMAND ----------

results_df.write.mode('overwrite').option('header',True).option("mergeSchema", "true").saveAsTable('keyword_matched_test')