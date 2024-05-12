from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_article_data(url):
    try:
        time.sleep(1)  # Sleep to reduce load on the server
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        return ' '.join(p.text for p in soup.find_all('p'))
    except Exception as e:
        print(f"Error fetching article data from {url}: {e}")
        return None

def scrape_dawn():
    """Scrape Dawn news website."""
    main_url = 'https://www.dawn.com'
    response = requests.get(main_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    articles = soup.find_all('article', class_='story')
    data = []

    for article in articles:
        title_tag = article.find('h2', class_='story__title')
        if title_tag and title_tag.a:
            title = title_tag.get_text(strip=True)
            link = title_tag.a['href']
            full_text = fetch_article_data(link)
            if full_text:
                data.append({'title': title, 'url': link, 'text': full_text})

    df = pd.DataFrame(data)
    df.to_csv('dawn_news_data.csv', index=False)

def scrape_thenews():
    main_url = 'https://www.thenews.com.pk/'
    response = requests.get(main_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    main_story_left = soup.find('div', class_='main_story_left')
    data = []
    if main_story_left:
        ul_tags = main_story_left.find_all('ul')
        for ul in ul_tags:
            li_tags = ul.find_all('li')
            for li in li_tags:
                a_tag = li.find('a')
                if a_tag and a_tag['href']:
                    title = a_tag.get('title', '')
                    link = a_tag['href']
                    full_text = fetch_article_data(link)
                    if full_text:
                        data.append({'title': title, 'url': link, 'text': full_text})
    
    df = pd.DataFrame(data)
    df.to_csv('news_data.csv', index=False)

def preprocess_data():
    """Preprocess and clean the scraped data."""
    files_info = [
        ('dawn_news_data.csv', 'airflow_env/Data/cleaned_dawn_news_data.csv'),
        ('news_data.csv', 'airflow_env/Data/cleaned_news_data.csv')
    ]
    for file_path, output_path in files_info:
        df = pd.read_csv(file_path)
        df['title'] = df['title'].str.strip().str.lower().str.replace(' +', ' ', regex=True)
        df['text'] = df['text'].str.strip().str.lower().str.replace(' +', ' ', regex=True)
        df.to_csv(output_path, index=False)

with DAG('news_scraping_dag', default_args=default_args, schedule_interval='@daily') as dag:
    scrape_dawn_task = PythonOperator(
        task_id='scrape_dawn',
        python_callable=scrape_dawn
    )
    scrape_thenews_task = PythonOperator(
        task_id='scrape_thenews',
        python_callable=scrape_thenews
    )
    preprocess_data_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data
    )
    dvc_add = BashOperator(
    task_id='dvc_add',
    bash_command='cd /Users/abdullahmac/Desktop/FAST/MLOPS/Assignment 02/airflow_env && dvc add dawn_news_data.csv news_data.csv cleaned_dawn_news_data.csv cleaned_news_data.csv.dvc && git commit -am "Update datasets" && dvc push'
)


    scrape_dawn_task >> scrape_thenews_task >> preprocess_data_task >> dvc_add