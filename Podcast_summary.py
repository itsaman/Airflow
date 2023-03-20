from airflow.decorators import dag, task
import pendulum #to define start time and end time in airflow
import requests as rq
import xmltodict
import os

#to intract with sqlite databases
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


@dag(
    dag_id ='Podcast_summary', 
    schedule_interval='*/ * * * *',
    start_date = pendulum.datetime(2023,3,18),
    catchup = False
)

def Podcast_summary():

    #task 1: This is different from the below one as its an older way to make the task
    create_database = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """,
        sqlite_conn_id= 'podcasts'
    )

    #task 2: This below way is called task flow, it is turning the task into an operator 
    @task()
    def get_episodes():
        os.environ['NO_PROXY'] = "https://www.marketplace.org/feed/podcast/marketplace/"
        data = rq.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes


    podcast_epiosdes = get_episodes()
    #first create database will be executed and then podcast_episode will run, we are doing this because using old way to create task.
    create_database.set_downstream(podcast_epiosdes)

    #task3
    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id = 'podcasts')
        stored = hook.get_pandas_df("Select * from episodes;")
        new_episodes = []

        for episode in episodes:
            if episode["link"] not in stored["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])
        
        hook.insert_rows(table= "episodes", rows = new_episodes, target_fields=["link", "title", "published", "description", "filename"])

    load_episodes(podcast_epiosdes)

    @task()
    def download_episodes(episodes):
        os.environ['NO_PROXY'] = "https://www.marketplace.org/feed/podcast/marketplace/"
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join("episode", filename)
            if not os.path.exists(audio_path):
                print(f" Downloading {filename}")
                audio = rq.get(episode["enclosure"]["@url"])
                with open(audio_path , "wb+") as f:
                    f.write(audio.content)
    

    download_episodes(podcast_epiosdes)

summary = Podcast_summary()
