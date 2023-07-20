# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1 - Extract the text from the survey/social narrative
# MAGIC This step breaks the note into two components:
# MAGIC <br>
# MAGIC > 1. The free text, which represents purely unstructured text
# MAGIC <br>
# MAGIC > 2. The survey text, which represents the semi-structured text with keywords leading to potentially false positives.
# MAGIC <br>
# MAGIC
# MAGIC ##### Input
# MAGIC This notebook assumes that the notes have already been stored in an SQL table containing a column with the note text.
# MAGIC <br>
# MAGIC ##### Output
# MAGIC This notebook produces a new SQL table breaking the text column from the input into the following:
# MAGIC <br>
# MAGIC > 1. normalized_free_text, which the the extracted free text
# MAGIC <br>
# MAGIC > 2. normalized_text_survey, which is the extracted semi-structured text.

# COMMAND ----------

# MAGIC %run "./Function_Library"

# COMMAND ----------

# MAGIC %run "./Parameters"

# COMMAND ----------

# Set parameters from Parameters notebook
FileName = FileName_1
query_string = query_string_1
input_col = input_col_1

# COMMAND ----------

os.makedirs('/dbfs/tmp/',exist_ok=True)
shutil.copytree(RootDir+'/input','/dbfs/tmp/input',dirs_exist_ok=True)

# COMMAND ----------

keyword_list = ['social history socioeconomic history','social history narrative']
np.savetxt('/dbfs/tmp/'+FileName+'.txt', keyword_list, fmt='%s')

# COMMAND ----------

# Parameter set in Parameters notebook
notes_df = spark.sql(query_string)
notes_df.persist()

# COMMAND ----------

PhraseFile = 'dbfs:/tmp/'+FileName+'.txt'

pipeline = build_pipeline(phrase_file=PhraseFile, input_col=input_col, finish_flag=True)
results_df = pipeline.fit(notes_df).transform(notes_df)
    
results_df = results_df.withColumn('text_flag', F.when(F.size(F.array_distinct(F.col('output')))>1,1).otherwise(0))

results_df = results_df \
    .withColumn('normalized_text',F.col('normalizedDocument.result')[0].cast(StringType())) \
    .withColumn('begin_index',F.col('entity.begin')[0].cast(IntegerType()))\
    .withColumn('end_index',F.col('entity.end')[1].cast(IntegerType()))
    
results_df = results_df.withColumn('normalized_text_survey', F.col('normalized_text')[F.col('begin_index') + 1: F.col('end_index') - F.col('begin_index') + 1])

results_df = results_df.withColumn('normalized_free_text',
                                   F.when(F.size(F.array_distinct(F.col('output')))<2,F.col('normalized_text')) \
                                   .otherwise(F.concat( \
                                     F.col('normalized_text')[F.lit(0):F.col('begin_index')], \
                                     F.col('normalized_text')[F.col('end_index') + 1: F.length(F.col('normalized_text')) - F.col('end_index') + 1] \
                                 )))

# COMMAND ----------

results_df.select('patient_id_masked','Date','text_flag','normalized_text_survey','normalized_free_text').write.mode('overwrite').option("overwriteSchema", "true").option('header',True).saveAsTable('clinical_notes_processed_test')

# COMMAND ----------

