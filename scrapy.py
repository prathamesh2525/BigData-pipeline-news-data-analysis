import requests
from bs4 import BeautifulSoup
from newspaper import Article
from pyspark.sql import SparkSession
import datetime


spark = SparkSession.builder.appName("WebScraper").getOrCreate()


urls = [
    # urls of news websites
]

def scrape_using_newspaper(url):
    """Use newspaper3k to extract article text from a URL."""
    article = Article(url)
    article.download()
    article.parse()
    return article.text

def scrape_using_beautifulsoup(url):
    """Use BeautifulSoup to extract article text from a URL."""
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    paragraphs = soup.find_all('p')
    return ' '.join([p.get_text() for p in paragraphs])


scraped_data = []

for url in urls:
    try:
        article_text = scrape_using_newspaper(url) 
        scraped_data.append(article_text)
    except Exception as e:
        print(f"Error scraping {url}: {e}")


text_data = scraped_data

rdd = spark.sparkContext.parallelize(text_data)

hdfs_path = "hdfs://namenode_host:9000/user/hadoop/news_articles_text/"  # Define the Path of hadoop directory 

rdd.saveAsTextFile(hdfs_path)

spark.stop()
