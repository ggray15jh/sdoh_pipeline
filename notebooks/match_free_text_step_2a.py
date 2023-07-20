# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 2a - Keyword matching on the unstructured notes
# MAGIC This step matches keywords on the free text.
# MAGIC <br>
# MAGIC ##### Input
# MAGIC This notebook assumes that the notes have already processing by step 1 and used the normalized_free_text column
# MAGIC <br>
# MAGIC ##### Output
# MAGIC This notebook produces a new SQL table breaking with the sdoh_name+'_free_text_flag' added

# COMMAND ----------

# MAGIC %run "./Function_Library"

# COMMAND ----------

# MAGIC %run "./Parameters"

# COMMAND ----------

sdoh_name = sdoh_name_param
query_string = query_string_2a
input_col = input_col_2a

# COMMAND ----------

CutoffDict = {'homeless':15,'housing':1,'food':4,'transportation':1} 

notes_df = spark.sql(query_string)
     
print(f'##### Now processing {sdoh_name}')

PhraseFile0 = 'dbfs:/tmp/input/'+sdoh_name+'_definite.txt' 
PhraseFile1 = 'dbfs:/tmp/input/'+sdoh_name+'_maybe.txt' 
        
pipeline = build_pipeline(phrase_file=PhraseFile0, input_col=input_col, finish_flag=False)
 
results_df = pipeline.fit(notes_df).transform(notes_df)
    
finisher = Finisher() \
    .setInputCols(['entity']) \
    .setOutputCols('definite_phrase_'+sdoh_name) \
    .setCleanAnnotations(True)
    
results_df = finisher.transform(results_df)
    
pipeline = build_pipeline(phrase_file=PhraseFile1, input_col=input_col, finish_flag=False)
results_df = pipeline.fit(results_df).transform(results_df)
    
    
finisher = Finisher() \
    .setInputCols(['entity']) \
    .setOutputCols('possible_phrase_'+sdoh_name) \
    .setCleanAnnotations(True)
    
results_df = finisher.transform(results_df)
   
results_df = results_df.withColumn(sdoh_name+'_free_text_flag',
                               F.when(F.size('definite_phrase_'+sdoh_name) > 0,1)
                                .when(F.size('possible_phrase_'+sdoh_name)>=CutoffDict[sdoh_name],1)
                                .otherwise(0))
            
print('##### Write notes')
results_df.write.mode('overwrite').option("overwriteSchema", "true").option('header',True).saveAsTable('keyword_matched_test')

# COMMAND ----------

