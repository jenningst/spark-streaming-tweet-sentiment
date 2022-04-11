<p align='center'>
  <img width="600" src='https://github.com/jenningst/spark-streaming-tweet-sentiment/blob/main/images/souvik-banerjee-9Z-2Ktg6CIM-unsplash.jpg' alt='Twitter + Phone'>
</p>
<p align='center'>
  Photo by <a href="https://unsplash.com/@rswebsols?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Souvik Banerjee</a> on <a href="https://unsplash.com/s/photos/twitter?utm_source=unsplash&utm_medium=referral&utm_content=creditCopyText">Unsplash</a>
</p>

# Spark-Streaming Tweet Sentiment Analysis
A project for my Master's "Parallel &amp; Distributed Computing" class where we simulate streaming of 500K tweets and subsequent sentiment analysis.

# Motivation
The main drivers for this project were to combine parallel & distributed concepts/topics into a project that would simulate real-time tweet sentiment analysis. 

# Method & Results
To provide source data, we first utilize a model to collect 500,000 tweets from Twitter's streaming API. The tweet stream was filtered for the top 50 trending topics to limit conversations to prescribed domains of interest. Using the CSV file of streamed tweet data, we then implement a preprocessing pipeline for cleaning/preparing the tweet data for sentiment analysis and sentiment predictions. The pipeline also implements a custom Spark transformer to leverage the Vader model for its applicability in social media. Finally, we leverage a Naive Bayes estimator to attempt to predict sentiment based on the available features.

# Future Work
- Baseline modeling with other methods (Logistic Regression, Distributional (Word) Representations, etc.)
- Model tuning using grid search
