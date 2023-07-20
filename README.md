# sdoh_pipeline
A pipeline for identifying patients with social determinants of health (SDOH) from the free text notes.

The commit contains two directories, input and notebooks.

The input directory contains the keywords for the social determinents of health for the matching algorithm in step 2a. Each SDOH has a definite (for keywords definitely indicating an SDOH) and a maybe (for keywords that might and are parameterized with a cutoff).

The notebooks directory contains a series of notebooks to run the pipeline in Databricks Workflow. The pipeline consists of the following:

step 1 -> step 2a -> step 2b -> step 3

The preceding four notebook have additional explanations. 

Additionally, there is a Parameters notebook and a Function_Library notebook.

The parameters notebook contains the parameters used by other notebooks for the pipeline and provides a simple, single location to change certain features of the pipeline.

The Function_Library contains common functions used throughout the notebooks.

A note on environment:

Spark NLP 5.0.0 is used with Databricks 12.2 environemt. Installation of Spark NLP on Databricks is available in the following guide provided by John Snow Labs

https://sparknlp.org/docs/en/install
