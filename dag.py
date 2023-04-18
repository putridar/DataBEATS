from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDataOperator,
    BigQueryUpsertTableOperator,
    BigQueryInsertJobOperator,
    BigQueryGetDatasetOperator,
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import requests
import warnings
import json
from config import SERVICE_ACCOUNT, CIDS, SECRETS
import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pymongo
from dotenv import dotenv_values

# Use .env values
config = dotenv_values(".env")

# Initialize Firebase Admin SDK with your credentials
cred = credentials.Certificate("databeats-firebase.json")
firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

# Filter out all warnings
warnings.filterwarnings("ignore")

# Google Cloud Service Connection
# export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{"conn_type": "google-cloud-platform", "scope": "https://www.googleapis.com/auth/bigquery", "project": "is3107-381408", "num_retries": 5}'
SERVICE_ACCOUNT1 = SERVICE_ACCOUNT


# secret for SPOTIFY
cids = CIDS
secrets = SECRETS

AUTH_URL = "https://accounts.spotify.com/api/token"
# base URL of all Spotify API endpoints
BASE_URL = "https://api.spotify.com/v1/"


def get_headers(CLIENT_ID, CLIENT_SECRET):
    # POST
    auth_response = requests.post(
        AUTH_URL,
        {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
    )

    # convert the response to JSON
    auth_response_data = auth_response.json()

    # save the access token
    access_token = auth_response_data["access_token"]
    headers = {"Authorization": "Bearer {token}".format(token=access_token)}

    return headers


default_args = {
    "owner": "airflow",
}



# helper
def get_time():
    # get today's date in UTC timezone
    today = datetime.datetime.now().date()
    # get the start of the week (Monday)
    start_of_week = today - datetime.timedelta(days=today.weekday())
    # convert to Unix timestamp
    start_of_week_unix = int(start_of_week.strftime("%s"))
    return start_of_week_unix


def enrich_artist_from_other_df(artist_df, other_df):
    headers = get_headers(cids[1], secrets[1])
    artist_name = []
    artist_id = []
    popularity = []
    genre = []
    curr = list(artist_df["artist_id"])
    oth = list(set(other_df["artist_id"]))
    count = 0
    for y in range(0, len(oth), 50):
        x = oth[y : min(y + 50, len(oth))]
        x = ",".join(x)
        params = f"ids={x}"
        if x in curr:
            continue
        result = requests.get(BASE_URL + "artists?" + params, headers=headers)
        result = result.json()
        if "artists" not in result:
            continue
        else:
            result = result["artists"]
        artist_name.extend(list(map(lambda x: x["name"], result)))
        artist_id.extend(list(map(lambda x: x["id"], result)))
        popularity.extend(list(map(lambda x: x["popularity"], result)))
        genre.extend(list(map(lambda x: x["genres"], result)))
        count += 1

    artist_dataframe = pd.DataFrame(
        {
            "artist_id": artist_id,
            "artist_name": artist_name,
            "genre": genre,
            "popularity": popularity,
        }
    )
    artist_df = artist_df.append(artist_dataframe)
    return artist_df


def get_week_on_chart(df):
    df["timestamp"] = df["timestamp"].apply(
        lambda x: datetime.datetime.fromtimestamp(x)
    )
    df = df.sort_values(["track_id", "timestamp"])
    grouped = df.groupby("track_id")
    new_df = {}
    newest_timestamp = max(df["timestamp"])
    top_50_timestamp = {}
    for timestamp in list(set(df["timestamp"])):
        top_50 = (
            df[df["timestamp"] == timestamp]
            .drop_duplicates(subset=["track_id"])
            .sort_values("popularity", ascending=False)
            .head(50)
        )
        top_50_songs = list(top_50["track_id"])
        top_50_timestamp[timestamp] = top_50_songs

    for name, group in grouped:
        prev_timestamp = group.iloc[0]["timestamp"]
        new_df[name] = group.drop(["timestamp"], axis=1).iloc[-1].to_dict()
        new_df[name]["chart"] = 0
        for index, row in group.iterrows():
            diff = (row["timestamp"] - prev_timestamp).days
            if diff <= 7 and name in top_50_timestamp[row["timestamp"]]:
                new_df[name]["chart"] += 1
            else:
                new_df[name]["chart"] = 0
            prev_timestamp = row["timestamp"]
        diff = (newest_timestamp - prev_timestamp).days
        if diff < 7:
            new_df[name]["chart"] = new_df[name]["chart"]
        else:
            new_df[name]["chart"] = 0

    return pd.DataFrame(new_df).T.reset_index().drop(["index"], axis=1)


with DAG(
    "is3107_spotify_dag",
    default_args=default_args,
    description="DAG for Spotify Analysis",
    schedule_interval=None,
    start_date=datetime(2023, 3, 1),
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = __doc__

    YEAR = datetime.datetime.now().year

    def extract_track_data(**kwargs):
        # Set up variables
        album_id = []
        track_name = []
        popularity = []
        track_id = []
        artist_id = []
        timestamps = []
        headers = get_headers(cids[0], secrets[0])
        time = get_time()

        query = f"year:{YEAR}"
        for i in range(0, 1000, 50):
            params = {"q": query, "type": "track", "limit": 50, "offset": i}
            track_results = requests.get(
                f"{BASE_URL}search/", headers=headers, params=params
            ).json()
            for i, t in enumerate(track_results["tracks"]["items"]):
                if t["id"] in track_id:
                    continue
                artist_id.append(t["artists"][0]["id"])
                album_id.append(t["album"]["id"])
                track_name.append(t["name"])
                track_id.append(t["id"])
                popularity.append(t["popularity"])
                timestamps.append(time)

        track_dataframe = pd.DataFrame(
            {
                "track_id": track_id,
                "artist_id": artist_id,
                "track_name": track_name,
                "popularity": popularity,
                "album_id": album_id,
                "timestamp": timestamps,
            }
        )
        # Push to XCom for second task
        ti = kwargs["ti"]
        track_dataframe_json = track_dataframe.to_json(orient="split")
        ti.xcom_push(key="track_dataframe", value=track_dataframe_json)

        print("track dataframe: ", track_dataframe.shape)

    def extract_artist_data(**kwargs):
        # Set up variables
        artist_name = []
        artist_id = []
        popularity = []
        genre = []
        timestamps = []
        headers = get_headers(cids[4], secrets[4])
        time = get_time()

        query = f"year:{YEAR}"
        for i in range(0, 1000, 50):
            try:
                params = {"q": query, "type": "artist", "limit": 50, "offset": i}
                results = requests.get(
                    f"{BASE_URL}search/", headers=headers, params=params
                ).json()
            except:
                continue
            if "artists" not in results:
                continue
            for i, t in enumerate(results["artists"]["items"]):
                if t["id"] in artist_id:
                    continue
                artist_name.append(t["name"])
                artist_id.append(t["id"])
                popularity.append(t["popularity"])
                genre.append(t["genres"])
                timestamps.append(time)

        artist_df = pd.DataFrame(
            {
                "artist_id": artist_id,
                "artist_name": artist_name,
                "genre": genre,
                "popularity": popularity,
                "timestamp": timestamps,
            }
        )

        # Push to XCom for second task
        ti = kwargs["ti"]
        artist_df_json = artist_df.to_json(orient="split")
        ti.xcom_push(key="artist_dataframe", value=artist_df_json)

        print("artist dataframe: ", artist_df.shape)

    def extract_album_data(**kwargs):
        # Set up variables
        artist_id = []
        album_name = []
        release_date = []
        total_tracks = []
        genre = []
        popularity = []
        album_id = []
        timestamps = []
        headers = get_headers(cids[1], secrets[1])
        time = get_time()

        query = f"year:{YEAR}"
        for i in range(0, 1000, 50):
            params = {"q": query, "type": "album", "limit": 50, "offset": i}
            album_results = requests.get(
                f"{BASE_URL}search/", headers=headers, params=params
            ).json()
            # album_results = sp.search(q='year:2023', type='album', market='SG', limit=50,offset=i)
            items = album_results["albums"]["items"]
            items = list(filter(lambda x: x, items))
            if items:
                album_id.extend(list(map(lambda x: x["id"], items)))
                artist_id.extend(list(map(lambda x: x["artists"][0]["id"], items)))
                album_name.extend(list(map(lambda x: x["name"], items)))
                release_date.extend(list(map(lambda x: x["release_date"], items)))
                total_tracks.extend(list(map(lambda x: x["total_tracks"], items)))

        j = 0
        headers = get_headers(cids[2], secrets[2])
        for id in album_id:
            albumId = id
            url = f"https://api.spotify.com/v1/albums/{albumId}"

            features = False
            pop = 0
            # Make the GET request
            try:
                features = requests.get(url, headers=headers).json()
                pop = features["popularity"]
            except Exception as e:
                print(e)

            popularity.append(pop)
            timestamps.append(time)

        # Create Dataframe
        album_dataframe = pd.DataFrame(
            {
                "album_id": album_id,
                "artist_id": artist_id,
                "album_name": album_name,
                "total_tracks": total_tracks,
                "release_date": release_date,
                "popularity": popularity,
                "timestamp": timestamps,
            }
        )
        print("album: ", album_dataframe.shape)
        ti = kwargs["ti"]
        album_json = album_dataframe.to_json(orient="split")
        ti.xcom_push(key="album_dataframe", value=album_json)

    def extract_audio_data(**kwargs):
        # Pull artist DataFrame from XCom
        # Pull from ti
        ti = kwargs["ti"]
        track_dataframe_json = ti.xcom_pull(
            task_ids="extract_track_data", key="track_dataframe"
        )
        track_dataframe = pd.read_json(track_dataframe_json, orient="split")

        audio_features = []
        headers = get_headers(cids[3], secrets[3])
        track_id = track_dataframe["track_id"]
        # Operations
        for y in range(0, len(track_id), 50):
            x = track_id[y : min(y + 50, len(track_id))]
            x = ",".join(x)
            params = f"ids={x}"
            url = f"{BASE_URL}audio-features?{params}"

            # Make the GET request
            features = requests.get(url, headers=headers).json()
            if "audio_features" not in features:
                continue
            # features = sp.audio_features(id)
            if features["audio_features"]:
                audio_features.extend(features["audio_features"])

        # Create DataFrame
        # audio_dataframe = pd.DataFrame(audio_features) -> leads to NoneType error
        audio_features_columns = list(track_dataframe[0].keys())
        audio_features_values = [list(x.values()) for x in track_dataframe if x != None]
        audio_dataframe = pd.DataFrame(
            audio_features_values, columns=audio_features_columns
        )
        audio_dataframe = (
            audio_dataframe.drop(["type"], axis=1)
            if "type" in audio_dataframe.columns
            else audio_dataframe
        )
        audio_dataframe = (
            audio_dataframe.drop(["error"], axis=1)
            if "error" in audio_dataframe.columns
            else audio_dataframe
        )
        print("audio: ", audio_dataframe.shape)

        df_json = audio_dataframe.to_json(orient="split")
        ti.xcom_push(key="audio_dataframe", value=df_json)

    # def extract_db(name):
    #     ref = db.collection(name)
    #     data = []
    #     for doc in ref.stream():
    #         doc_data = doc.to_dict()
    #         data.append(doc_data)
    #     return data

    def extract_db(name):
        myclient = pymongo.MongoClient(config["ATLAS_URI"])
        mydb = myclient["DataBeats"]
        mycol = mydb[name]
        data = []
        for doc in mycol.find():
            data.append(doc)
        return data
    
    def extract_artist_db(**kwargs):
        ti = kwargs["ti"]
        data = extract_db("Artists")
        df = pd.DataFrame(data)
        df_json = df.to_json(orient="split")
        ti.xcom_push(key="artist_db", value=df_json)

    def extract_album_db(**kwargs):
        ti = kwargs["ti"]
        data = extract_db("Albums")
        df = pd.DataFrame(data)
        df_json = df.to_json(orient="split")
        ti.xcom_push(key="album_db", value=df_json)

    def extract_track_db(**kwargs):
        ti = kwargs["ti"]
        data = extract_db("Tracks")
        df = pd.DataFrame(data)
        df_json = df.to_json(orient="split")
        ti.xcom_push(key="track_db", value=df_json)

    def extract_audio_db(**kwargs):
        ti = kwargs["ti"]
        data = extract_db("Audio")
        df = pd.DataFrame(data)
        df_json = df.to_json(orient="split")
        ti.xcom_push(key="audio_db", value=df_json)

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        track_dataframe_json = ti.xcom_pull(
            task_ids="extract_track_data", key="track_dataframe"
        )
        track_df = pd.read_json(track_dataframe_json, orient="split")

        album_dataframe_json = ti.xcom_pull(
            task_ids="extract_album_data", key="album_dataframe"
        )
        album_df = pd.read_json(album_dataframe_json, orient="split")

        artist_dataframe_json = ti.xcom_pull(
            task_ids="extract_artist_data", key="artist_dataframe"
        )
        artist_df = pd.read_json(artist_dataframe_json, orient="split")

        audio_dataframe_json = ti.xcom_pull(
            task_ids="extract_audio_data", key="audio_dataframe"
        )
        audio_df = pd.read_json(audio_dataframe_json, orient="split")

        track_db_json = ti.xcom_pull(task_ids="extract_track_db", key="track_db")
        track = pd.read_json(track_db_json, orient="split")

        album_db_json = ti.xcom_pull(task_ids="extract_album_db", key="album_db")
        album = pd.read_json(album_db_json, orient="split")

        artist_db_json = ti.xcom_pull(task_ids="extract_artist_db", key="artist_db")
        artist = pd.read_json(artist_db_json, orient="split")

        audio_db_json = ti.xcom_pull(task_ids="extract_audio_db", key="audio_db")
        audio = pd.read_json(audio_db_json, orient="split")

        artist_df = artist_df[artist_df["popularity"] != 0]
        album_df = album_df[album_df["popularity"] != 0].reset_index()
        track_df = track_df[track_df["popularity"] != 0].reset_index()

        artist = artist.append(artist_df)
        album = album.append(album_df)
        track = track.append(track_df)
        audio = audio.append(audio_df)

        audio = audio.rename(columns={"id": "track_id"})
        merged_df = pd.merge(track, audio, on="track_id", how="left").drop_duplicates(
            subset=["track_id", "timestamp"]
        )
        df = get_week_on_chart(merged_df)

        artist_col = ["artist_id", "artist_name", "popularity"]
        album_col = ["album_id", "artist_id", "album_name", "popularity"]
        exclude_track_col = ["uri", "track_href"]
        artist = artist[artist_col].drop_duplicates()
        album = album[album_col].drop_duplicates()
        df = df.drop(exclude_track_col, axis=1).drop_duplicates()
        print("artist: ", artist_df.shape)
        print("album: ", album_df.shape)
        print("track: ", df.shape)

        df_json = artist_df.to_json(orient="split")
        ti.xcom_push(key="artist_db", value=df_json)
        df_json = album_df.to_json(orient="split")
        ti.xcom_push(key="album_db", value=df_json)
        df_json = track_df.to_json(orient="split")
        ti.xcom_push(key="track_db", value=df_json)
        df_json = audio_df.to_json(orient="split")
        ti.xcom_push(key="audio_db", value=df_json)

        df_json = artist.to_json(orient="split")
        ti.xcom_push(key="artist_dataframe", value=df_json)
        df_json = album.to_json(orient="split")
        ti.xcom_push(key="album_dataframe", value=df_json)
        df_json = df.to_json(orient="split")
        ti.xcom_push(key="track_dataframe", value=df_json)

    # Firebase
    # def load_track_db(**kwargs):
    #     ti = kwargs["ti"]
    #     track_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="track_db")
    #     track = pd.read_json(track_dataframe_json, orient="split")

    #     for row in range(track.shape[0]):
    #         curr = track.iloc[track.shape[0] - 1 - row]
    #         id = curr["track_id"]
    #         time = get_time()
    #         art_id = curr["artist_id"]
    #         alb_id = curr["album_id"]
    #         name = curr["track_name"]
    #         pop = curr["popularity"]
    #         data = {
    #             "track_id": id,
    #             "album_id": alb_id,
    #             "artist_id": art_id,
    #             "track_name": name,
    #             "popularity": int(pop),
    #             "timestamp": time,
    #         }
    #         db.collection("tracks").document(id + "_" + str(time)).set(data)

    # MongoDB
    def uploadTracksToMongo(**kwargs):
        ti = kwargs["ti"]
        myclient = pymongo.MongoClient(config["ATLAS_URI"])
        mydb = myclient["DataBeats"]
        collection = mydb["Tracks"]

        track_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="track_db")
        track = pd.read_json(track_dataframe_json, orient="split")
        
        for row in range(track.shape[0]):
            curr = track.iloc[track.shape[0] - 1 - row]
            id = curr["track_id"]
            time = get_time()
            art_id = curr["artist_id"]
            alb_id = curr["album_id"]
            name = curr["track_name"]
            pop = curr["popularity"]
            data = {
                "track_id": id,
                "album_id": alb_id,
                "artist_id": art_id,
                "track_name": name,
                "popularity": int(pop),
                "timestamp": time,
            }
            collection.insert_one(data)

    # Firebase
    # def load_album_db(**kwargs):
    #     ti = kwargs["ti"]
    #     album_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="album_db")
    #     album = pd.read_json(album_dataframe_json, orient="split")
    #     for row in range(album.shape[0]):
    #         curr = album.iloc[album.shape[0] - 1 - row]
    #         id = curr["album_id"]
    #         time = get_time()
    #         art_id = curr["artist_id"]
    #         name = curr["album_name"]
    #         t = curr["total_tracks"]
    #         d = curr["release_date"]
    #         pop = curr["popularity"]
    #         data = {
    #             "album_id": id,
    #             "artist_id": art_id,
    #             "album_name": name,
    #             "total_tracks": int(t),
    #             "release_date": d,
    #             "popularity": int(pop),
    #             "timestamp": time,
    #         }
    #         db.collection("albums").document(id + "_" + str(time)).set(data)

    # MongoDB
    def uploadAlbumToMongo(**kwargs):
        ti = kwargs["ti"]
        myclient = pymongo.MongoClient(config["ATLAS_URI"])
        mydb = myclient["DataBeats"]
        collection = mydb["Albums"]

        album_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="album_db")
        album = pd.read_json(album_dataframe_json, orient="split")

        for row in range(album.shape[0]):
            curr = album.iloc[album.shape[0] - 1 - row]
            id = curr["album_id"]
            time = get_time()
            art_id = curr["artist_id"]
            name = curr["album_name"]
            t = curr["total_tracks"]
            d = curr["release_date"]
            pop = curr["popularity"]
            data = {
                "album_id": id,
                "artist_id": art_id,
                "album_name": name,
                "total_tracks": int(t),
                "release_date": d,
                "popularity": int(pop),
                "timestamp": time,
            }
            collection.insert_one(data)

    # Firebase
    # def load_artist_db(**kwargs):
    #     ti = kwargs["ti"]
    #     artist_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="artist_db")
    #     artist = pd.read_json(artist_dataframe_json, orient="split")
    #     for row in range(artist.shape[0]):
    #         curr = artist.iloc[artist.shape[0] - 1 - row]
    #         id = curr["artist_id"]
    #         time = get_time()
    #         name = curr["artist_name"]
    #         genre = curr["genre"]
    #         pop = curr["popularity"]
    #         data = {
    #             "artist_id": id,
    #             "artist_name": name,
    #             "genre": genre,
    #             "popularity": int(pop),
    #             "timestamp": time,
    #         }
    #         db.collection("artists").document(id + "_" + str(time)).set(data)

    # MongoDB
    def uploadArtistToMongo(**kwargs):
        ti = kwargs["ti"]
        myclient = pymongo.MongoClient(config["ATLAS_URI"])
        mydb = myclient["DataBeats"]
        collection = mydb["Artists"]
        
        artist_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="artist_db")
        artist = pd.read_json(artist_dataframe_json, orient="split")
        for row in range(artist.shape[0]):
            curr = artist.iloc[artist.shape[0] - 1 - row]
            id = curr["artist_id"]
            time = get_time()
            name = curr["artist_name"]
            genre = curr["genre"]
            pop = curr["popularity"]
            data = {
                "artist_id": id,
                "artist_name": name,
                "genre": genre,
                "popularity": int(pop),
                "timestamp": time,
            }
            collection.insert_one(data)

    # Firebase
    # def load_audio_db(**kwargs):
    #     ti = kwargs["ti"]
    #     audio_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="audio_db")
    #     audio = pd.read_json(audio_dataframe_json, orient="split")
    #     for row in range(audio.shape[0]):
    #         curr = audio.iloc[row]
    #         data = {}
    #         id = curr["id"]
    #         data = curr.to_dict()
    #         data["track_id"] = id
    #         del data["id"]
    #         del data["type"]
    #         db.collection("audio").document(id).set(data)

    # MongoDB
    def uploadAudioToMongo(**kwargs):
        ti = kwargs["ti"]
        myclient = pymongo.MongoClient(config["ATLAS_URI"])
        mydb = myclient["DataBeats"]
        collection = mydb["Audio"]
        
        audio_dataframe_json = ti.xcom_pull(task_ids="transform_data", key="audio_db")
        audio = pd.read_json(audio_dataframe_json, orient="split")
        for row in range(audio.shape[0]):
            curr = audio.iloc[row]
            data = {}
            id = curr["id"]
            data = curr.to_dict()
            data["track_id"] = id
            del data["id"]
            del data["type"]
            collection.insert_one(data)

    def load_tracks_data(**kwargs):
        # Connect to BigQuery
        client = bigquery.Client()

        # Pull Tracks Data from previous task
        ti = kwargs["ti"]
        json_tracks_df = ti.xcom_pull(task_ids="transform", key="track_dataframe")
        tracks_df = json.loads(json_tracks_df)
        tracks_df_fix = pd.json_normalize(tracks_df, record_path=["data"])
        tracks_df_fix.columns = [
            "track_id",
            "artist_id",
            "track_name",
            "popularity",
            "album_id",
            "danceability",
            "energy",
            "key",
            "loudness",
            "mode",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo",
            "analysis_url",
            "duration_ms",
            "time_signature",
            "chart",
        ]
        print(tracks_df_fix)

        table_id = "is3107-381408.Spotify.Tracks"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
            tracks_df_fix, table_id, job_config=job_config
        )
        job.result()
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def load_artists_data(**kwargs):
        # Connect to BigQuery
        client = bigquery.Client()

        # Pull Tracks Data from previous task
        ti = kwargs["ti"]
        json_artists_df = ti.xcom_pull(task_ids="transform", key="artist_dataframe")
        artists_df = json.loads(json_artists_df)
        artists_df_fix = pd.json_normalize(artists_df, record_path=["data"])
        artists_df_fix.columns = ["artist_id", "artist_name", "popularity"]
        print(artists_df_fix)

        table_id = "is3107-381408.Spotify.Artists"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
            artists_df_fix, table_id, job_config=job_config
        )
        job.result()
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def load_albums_data(**kwargs):
        # Connect to BigQuery
        client = bigquery.Client()

        # Pull Albums Data from previous task
        ti = kwargs["ti"]
        json_albums_df = ti.xcom_pull(task_ids="transform", key="album_dataframe")
        albums_df = json.loads(json_albums_df)
        albums_df_fix = pd.json_normalize(albums_df, record_path=["data"])
        albums_df_fix.columns = [
            "album_id",
            "artist_id",
            "album_name",
            "popularity",
        ]
        print(albums_df_fix)

        table_id = "is3107-381408.Spotify.Albums"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(
            albums_df_fix, table_id, job_config=job_config
        )
        job.result()
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def remove_duplicates(target, **context):
        return f"""
            CREATE OR REPLACE TABLE `{target}`
            AS
            SELECT
            DISTINCT *
            FROM `{target}`
            group by 1
        """
    
    def truncate_table(target, **context):
        return f"""
            TRUNCATE TABLE {target}
        """

    extract_track_task = PythonOperator(
        task_id="extract_track_data",
        python_callable=extract_track_data,
    )

    extract_artist_task = PythonOperator(
        task_id="extract_artist_data",
        python_callable=extract_artist_data,
    )

    extract_album_task = PythonOperator(
        task_id="extract_album_data",
        python_callable=extract_album_data,
    )

    extract_audio_task = PythonOperator(
        task_id="extract_audio_data",
        python_callable=extract_audio_data,
    )

    extract_artist_db_task = PythonOperator(
        task_id="extract_artist_db",
        python_callable=extract_artist_db,
    )

    extract_album_db_task = PythonOperator(
        task_id="extract_album_db",
        python_callable=extract_album_db,
    )

    extract_track_db_task = PythonOperator(
        task_id="extract_track_db",
        python_callable=extract_track_db,
    )

    extract_audio_db_task = PythonOperator(
        task_id="extract_audio_db",
        python_callable=extract_audio_db,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    # load_track_db_task = PythonOperator(
    #     task_id="load_track_db",
    #     python_callable=load_track_db,
    #     dag=dag,
    # )

    # load_artist_db_task = PythonOperator(
    #     task_id="load_artist_db",
    #     python_callable=load_artist_db,
    #     dag=dag,
    # )

    # load_album_db_task = PythonOperator(
    #     task_id="load_album_db",
    #     python_callable=load_album_db,
    #     dag=dag,
    # )

    # load_audio_db_task = PythonOperator(
    #     task_id="load_audio_db",
    #     python_callable=load_audio_db,
    #     dag=dag,
    # )

    load_tracks_mongo_task = PythonOperator(
        task_id='load_tracks_mongo',
        python_callable=uploadTracksToMongo,
        dag=dag,
    )

    load_albums_mongo_task = PythonOperator(
        task_id='load_albums_mongo',
        python_callable=uploadAlbumToMongo,
        dag=dag,
    )

    load_artists_mongo_task = PythonOperator(
        task_id='load_artists_mongo',
        python_callable=uploadArtistToMongo,
        dag=dag,
    )

    load_audio_mongo_task = PythonOperator(
        task_id='load_audio_mongo',
        python_callable=uploadAudioToMongo,
        dag=dag,
    )

    load_tracks_task = PythonOperator(
        task_id="load_tracks_data",
        python_callable=load_tracks_data,
        dag=dag,
    )

    load_artists_task = PythonOperator(
        task_id="load_artists_data",
        python_callable=load_artists_data,
        dag=dag,
    )

    load_albums_task = PythonOperator(
        task_id="load_albums_data",
        python_callable=load_albums_data,
        dag=dag,
    )

    truncate_table_tracks = BigQueryOperator(
        task_id="truncate_table_tracks",
        sql=truncate_table("is3107-381408.Spotify.Tracks"),
        destination_dataset_table="is3107-381408.Spotify.Tracks",
        # bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
    )

    truncate_table_artists = BigQueryOperator(
        task_id="truncate_table_artists",
        sql=truncate_table("is3107-381408.Spotify.Artists"),
        destination_dataset_table="is3107-381408.Spotify.Artists",
        # bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
    )

    truncate_table_albums = BigQueryOperator(
        task_id="truncate_table_albums",
        sql=truncate_table("is3107-381408.Spotify.Albums"),
        destination_dataset_table="is3107-381408.Spotify.Albums",
        # bigquery_conn_id='bigquery_default',
        use_legacy_sql=False,
    )

    (
        (
            extract_track_task,
            extract_artist_task,
            extract_album_task,
            extract_artist_db_task,
            extract_album_db_task,
            extract_track_db_task,
            extract_audio_db_task,
        )
        >> extract_audio_task
        >> transform_task
        >> (
            load_tracks_mongo_task,
            load_albums_mongo_task,
            load_artists_mongo_task,
            load_audio_mongo_task,
        )
        >> truncate_table_tracks
        >> truncate_table_artists
        >> truncate_table_albums
        >> load_tracks_task
        >> load_artists_task
        >> load_albums_task
    )
