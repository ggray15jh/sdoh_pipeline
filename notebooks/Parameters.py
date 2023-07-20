# Databricks notebook source
# MAGIC %md
# MAGIC ###Parameter Dictionary
# MAGIC #### Universal Params
# MAGIC sdoh_name_param-> the name for the social determinant of health being investigated. Must be homeless, food, housing or transportation.
# MAGIC <br>
# MAGIC #### Step 1 Params
# MAGIC **RootDir**->the location of the pipeline directory on the system.
# MAGIC <br>
# MAGIC **FileName_1**->File name to write the text for regex extraction.
# MAGIC <br>
# MAGIC **query_string_1**-> This is the SQL statement querying the first database containing the unprocessed patient notes.
# MAGIC <br>
# MAGIC **input_col_1**-> Name of the column from the queried database containing the text for patient notes.
# MAGIC <br>
# MAGIC #### Step 2a Params
# MAGIC **query_string_2a**-> SQL query for the processed databse from Step 1.
# MAGIC <br>
# MAGIC **input_col_2a**-> Input name for the free text.
# MAGIC #### Step 2b Params
# MAGIC **query_string_2b**-> SQL query for the processed database from Step 1.
# MAGIC <br>
# MAGIC **rules_2b**-> Regex ruls for extracting answers from the surveys.
# MAGIC <br>
# MAGIC **input_col_2b**-> Input column for the extracted surveys.
# MAGIC #### Step 3 Params
# MAGIC **query_string_3**-> SQL query for the database generated from Step 2b

# COMMAND ----------

# Universal Params
sdoh_name_param = 'homeless'

# Step 1
RootDir = '/Workspace/Users/ggray15@jh.edu/cphit_nlp/pipeline'
FileName_1 = 'keyword_list_extracts'
query_string_1 = 'select * from clinical_notes limit 1000'
input_col_1 = 'Text'

# Step 2a
query_string_2a = 'select * from clinical_notes_processed_test'
input_col_2a = 'normalized_free_text'

# Step 2b
query_string_2b = 'select * from keyword_matched_test'
rules_2b = "(?<=food\sinsecurity\sworry).*?(?=food\sinsecurity\sinability), food insecurity worry phrase\n(?<=food\sinsecurity\sinability).*?(?=transportation\sneeds\smedical), food insecurity inability phrase\n(?<=transportation\sneeds\smedical).*?(?=transportation\sneeds\snon\smedical), transportation needs medical phrase\n(?<=transportation\sneeds\snon\smedical).*?(?=occupational\shistory), transportation needs non medical phrase"
input_col_2b = 'normalized_text_survey'

# Step 3
query_string_3 = 'select * from keyword_matched_test'