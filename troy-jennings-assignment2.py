# Databricks notebook source
! python -m pip install --upgrade pip
! pip install TextBlob

# COMMAND ----------

import nltk
import os
import re

from collections import Counter
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import WordNetLemmatizer
from pyspark import keyword_only
from pyspark.context import SparkContext
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer, Word2Vec, StringIndexer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, HasOutputCols, Param, Params, TypeConverters
from pyspark.ml.feature import SQLTransformer, StopWordsRemover, Tokenizer, CountVectorizer, HashingTF, IDF
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructType, StructField, LongType, StringType, FloatType, TimestampType, IntegerType
from textblob import TextBlob

sc = SparkContext.getOrCreate()

# COMMAND ----------

# download stems and sentiment lexicon
nltk.download('wordnet')
nltk.download('vader_lexicon')

# COMMAND ----------

DATA_PATH = '/FileStore/tables/training_tweets-1.csv'
STREAMING_PATH = '/tmp/stream_tweets/train'

# define schema, read in data, and partition it to train/test
tweet_schema = StructType([
  StructField('author_id', LongType(), False),
  StructField('tweet_created_at', TimestampType(), False),
  StructField('tweet_id', LongType(), False),
  StructField('tweet_text', StringType(), True),
])

tweet_df = spark.read.format('csv') \
                .schema(tweet_schema) \
                .option('header', True) \
                .option('mode', 'dropmalformed') \
                .load(DATA_PATH)

# create a sample of 50K tweets to keep running times low
data = tweet_df.sample(withReplacement=False, fraction=0.1, seed=3)
train, test = data.randomSplit(weights=[0.7, 0.3], seed=42)

# COMMAND ----------

print(f'Train shape: ({train.count()}, {len(train.columns)})')
print(f'Test shape: ({test.count()}, {len(test.columns)})')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Machine Learning

# COMMAND ----------

class SentimentTransformer(Transformer, HasInputCol, HasOutputCol, HasOutputCols):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, outputCols=None):
        super().__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, outputCols=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCol(self, new_inputCol):
        return self.setParams(inputCol=new_inputCol)

    def setOutputCol(self, new_outputCol):
        return self.setParams(outputCol=new_outputCol)
      
    def setOutputCols(self, new_outputCols):
        return self.setParams(outputCols=new_outputCols)

    def _transform(self, dataset):
      if not self.isSet('inputCol'):
          raise ValueError('No input column set for the SentimentTransformer transformer.')

      input_col = self.getInputCol()
      output_col = self.getOutputCol()
      
      def _score_sentiment(tokens):
        '''Return a list of the neutral, negative, and positive sentiment values.'''
        score = TextBlob(' '.join(tokens))
        if score.sentiment.polarity < 0:
          return 'negative'
        elif score.sentiment.polarity == 0:
          return 'neutral'
        else:
          return 'positive'
      
      # add sentiment scores to the dataset
      scores = f.udf(lambda s: _score_sentiment(s), StringType())
      return dataset.withColumn(output_col, scores(input_col))

# COMMAND ----------

# ******************************************** #
# **********    CUSTOM FUNCTIONS    ********** #
# ******************************************** #

def is_retweet(tweet) -> str:
  '''Indicates whether the tweet is a retweet.'''
  retweet = False
  if re.search(r'^RT ', tweet):
    retweet = True
  return retweet

def count_mentions(tweet) -> int:
  '''Count the number of mentions (e.g. @TwitterDev) in the tweet text.'''
  mention_matches = re.findall(r'@[\w]+', tweet)
  return len(mention_matches)

def count_links(tweet) -> int:
  '''Count the number of links in the tweet'''
  link_matches = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', tweet)
  return len(link_matches)

def get_uppercase_percentage(tweet) -> int:
  '''Calculate the percentage of uppercase to lowercase letters.'''
  uppercase_matches = re.findall(r'[A-Z]', tweet)
  return len(uppercase_matches) / len(tweet)

def clean_tweet_text(tweet) -> str:
  '''Cleanses a tweet text of user mentions, links, hashtags, special characters, and emojis.'''

  replacement_patterns = [
      r'(@[\w]+)',
      r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
      r'(#\w+)',
      r'^(RT )',
      r'[\$&+,:;=?@#|\<>.^*()%!-/]',
      r'\n',
      r'[0-9]+'
  ]
  clean_text = re.sub(r'|'.join(replacement_patterns), '', tweet, 99)

  return clean_text.encode(encoding='ascii', errors='ignore').decode('ascii').strip()

def simple_tokenize_text(tweet) -> list:
  '''Tokenizes the tweet text (with support for multiple spaces in-string)'''
  return [ t.lower() for t in tweet.split() ]

def lemmatize_tokens(tokens) -> list:
  '''Returns the base form of each word in a list of tokens.'''
  wn_lemmatizer = WordNetLemmatizer()
  return [ wn_lemmatizer.lemmatize(word, pos='v') for word in tokens ]


# ******************************************** #
# ********** USER-DEFINED FUNCTIONS ********** #
# ******************************************** #

clean_text_udf = spark.udf.register('clean_text_udf', lambda row: clean_tweet_text(row), StringType())
tokenizer_udf = spark.udf.register('tokenizer_udf', lambda row: simple_tokenize_text(row), ArrayType(StringType()))
lemmatizer_udf = spark.udf.register('lemmatizer_udf', lambda row: lemmatize_tokens(row), ArrayType(StringType()))
# negative_indicator_udf = spark.udf.register('negative_indicator_udf', lambda row: classify_negative_sentiment(row), FloatType())

# ******************************************** #
# **********      TRANSFORMERS      ********** #
# ******************************************** #

filter_na = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE tweet_id IS NOT NULL AND tweet_text IS NOT NULL")
clean_text_transformer = SQLTransformer(statement="SELECT *, clean_text_udf(tweet_text) AS cleansed FROM __THIS__")
filter_nulls_transformer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cleansed IS NOT NULL AND cleansed != ''")
tokenizer = SQLTransformer(statement="SELECT *, tokenizer_udf(cleansed) as tokens FROM __THIS__")
stop_remover = StopWordsRemover(stopWords=StopWordsRemover().getStopWords()).setInputCol('tokens').setOutputCol('stops_removed')
sentiment_scorer = SentimentTransformer().setInputCol('lemmatized').setOutputCol('sentiment')
prediction_indexer = StringIndexer(inputCol='sentiment', outputCol='label')
lemmatizer = SQLTransformer(statement="SELECT *, lemmatizer_udf(stops_removed) AS lemmatized FROM __THIS__")
hashing_tf = HashingTF().setInputCol('lemmatized').setOutputCol('raw_features').setNumFeatures(20)
idf = IDF().setInputCol('raw_features').setOutputCol('features')
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

# COMMAND ----------

# Create a training pipeline
preproc_pipeline = Pipeline(stages=[
  filter_na,
  clean_text_transformer,
  filter_nulls_transformer,
  tokenizer,
  stop_remover,
  lemmatizer,
  hashing_tf,
  idf,
  sentiment_scorer,
  prediction_indexer,
  nb
])

# build the model
nb_model = preproc_pipeline.fit(train)
train_results = nb_model.transform(train)

# COMMAND ----------

# evaluate training data
classificationEval = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName="accuracy")
train_eval = classificationEval.evaluate(train_results)
print(f'Training Classification Score (Accuracy) is {train_eval}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming

# COMMAND ----------

# do date preprocessing to partition by hour
hour_udf = f.udf(lambda x: x.hour, IntegerType())
train = train.withColumn('hour', hour_udf('tweet_created_at'))

# write to disk
train.write.option('header', True).partitionBy('hour').csv(STREAMING_PATH)

# COMMAND ----------

# stream in additional tweets
streaming_schema = StructType([
  StructField('author_id', LongType(), False),
  StructField('tweet_created_at', TimestampType(), False),
  StructField('tweet_id', LongType(), False),
  StructField('tweet_text', StringType(), True),
  StructField('hour', IntegerType(), True),
])

# source
tweet_stream = spark.readStream.format('csv') \
                    .option('header', True) \
                    .schema(streaming_schema) \
                    .option('mode', 'dropMalformed') \
                    .option('maxFilesPerTrigger', 1) \
                    .load(STREAMING_PATH)

# query
predict_tweet_sentiment = nb_model.transform(tweet_stream).select('tweet_id', 'probability', 'prediction')

# sink
sink_stream = predict_tweet_sentiment.writeStream.outputMode('append') \
                    .format('memory') \
                    .queryName('predict_tweet_sentiment') \
                    .trigger(processingTime='5 seconds') \
                    .start()

# COMMAND ----------

spark.sql('SELECT SUM(CASE WHEN prediction = 1.0 THEN 1 ELSE 0 END) AS positive, SUM(CASE WHEN prediction = 0.0 THEN 1 ELSE 0 END) AS neutral, SUM(CASE WHEN prediction = 2.0 THEN 1 ELSE 0 END) AS negative  from predict_tweet_sentiment').show()
