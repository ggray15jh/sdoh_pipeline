# Databricks notebook source
from pyspark.sql.types import StringType, IntegerType
#Spark NLP
import pandas as pd
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from sparknlp.annotator import *
from sparknlp.base import *
import pyspark.sql.functions as F
from sparknlp import Finisher
import numpy as np
import os
import shutil

# COMMAND ----------

def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)

def build_pipeline(phrase_file='', model_type='textMatcher', input_col='', finish_flag=True):
  # Custom pipeline
    documentAssembler = DocumentAssembler().setInputCol(input_col).setOutputCol('document')
    documentNormalizer = DocumentNormalizer() \
      .setInputCols("document") \
      .setOutputCol("normalizedDocument") \
      .setAction("clean") \
      .setPatterns(["""[^0-9a-zA-Z]+"""]) \
      .setLowercase(True)
    sentenceDetector = SentenceDetector().setInputCols(['normalizedDocument']).setOutputCol('sentences')
    tokenizer = Tokenizer().setInputCols(['sentences']).setOutputCol('tokens')
    normalizer = Normalizer() \
      .setInputCols(["tokens"]) \
      .setOutputCol("normalized") \
      .setLowercase(True) \
      .setCleanupPatterns(["""[^0-9a-zA-Z]+"""])
    
    if model_type == 'textMatcher':
        model = TextMatcher().setInputCols('normalizedDocument', 'normalized') \
            .setEntities(phrase_file, ReadAs.TEXT) \
            .setOutputCol('entity') \
            .setCaseSensitive(False)
        
        if finish_flag:
            finisher = Finisher() \
                .setInputCols(['entity']) \
                .setOutputCols('output') \
                .setCleanAnnotations(False)
            
    elif model_type == 'ngram':
        model = NGramGenerator() \
            .setInputCols(['normalized']) \
            .setOutputCol('ngrams') \
            .setN(4)
        
        if finish_flag:
            finisher = Finisher() \
                .setInputCols(['ngrams']) \
                .setOutputCols('output') \
                .setCleanAnnotations(False)
    else:
        print('Unrecognized/unsupported model type passed! Exiting')
        return None
  
    stages=[
        documentAssembler,
        documentNormalizer,
        sentenceDetector,
        tokenizer,
        normalizer,
        model
       ]
    
    if finish_flag:
        stages.append(finisher)
        
    nlpPipeline = Pipeline(stages=stages)
  
    return nlpPipeline

def run_pipeline(PhraseFile0, PhraseFile1, input_col, input_df):
    #####    
    pipeline = build_pipeline(phrase_file=PhraseFile0, input_col=input_col)
        
    start = timer()
    results_df_0 = pipeline.fit(input_df).transform(input_df)
    end = timer()
    dt = end - start
    print(f'>  Elapsed time to fit first pipeline was {dt} seconds')
        
    ##### 
    start = timer()
    results_df_0 = results_df_0.withColumn('definite_phrase',F.col('entity.result'))
    end = timer()
    dt = end - start
    print(f'>  Elapsed time to convert definite phrase list was {dt} seconds')
    
    pipeline = build_pipeline(phrase_file=PhraseFile1, input_col=input_col)
    
    #####
    start = timer()
    results_df_1 = pipeline.fit(results_df_0).transform(results_df_0)
    end = timer()
    dt = end - start
    print(f'>  Elapsed time to fit second pipeline was {dt} seconds')
    
    #####
    start = timer()
    results_df_1 = results_df_1.withColumn('possible_phrase',F.col('entity.result'))
    end = timer()
    dt = end - start
    print(f'>  Elapsed time convert possible phrase list was {dt} seconds')
    
    results_df_1 = results_df_1.withColumn(iKey+'_cohort_'+iCohort+'_flag',F.lit(1))
    
    #####
    start = timer()
    results_df_1 = results_df_1.withColumn(iKey+'_sdoh_'+iCohort+'_flag',
                                           F.when(F.size('definite_phrase') > 0,1)
                                           .when(F.size('possible_phrase')>=CutoffDict[iKey],1)
                                           .otherwise(0))
    
    end = timer()
    dt = end - start
    print(f'>  Elapsed time to calculate sdoh flag was {dt} seconds')
    
    return results_df_1

def ScoreKeywords(iList, definite_list, Cutoff):
    sdoh_flag = 0
    nCount = 0
    CountFlag = True
    if len(iList) > 0:
        for iRow in iList:
            iPhrase = iRow.__getitem__('result')
            if iPhrase in definite_list:
                sdoh_flag = 1
                CountFlag = False
                break
            else:
                nCount += 1
        if CountFlag:
            if nCount >= Cutoff:
                sdoh_flag = 1
            else:
                sdoh_flag = 0
        else:
            pass
    else:
        sdoh_flag = 0
        
    return sdoh_flag

def ReturnScore(definite_list, Cutoff):
    return udf(lambda x: ScoreKeywords(x, definite_list, Cutoff), IntegerType())