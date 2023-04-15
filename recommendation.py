import pandas as pd
import joblib
from sklearn.cluster import KMeans
import statistics
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler

le = LabelEncoder()
scaler = StandardScaler()

track = joblib.load('track_df.pkl') # to be replaced from bigquery
audio = joblib.load('audio_df.pkl') # to be replaced from bigquery

def preprocess(track, audio):
    track = track.rename(columns={"track_id":"id"})
    track['id'] = track['id'].astype(str)
    audio['id'] = audio['id'].astype(str)
    merged_df = pd.merge(track, audio, on='id', how='left')
    merged_df = merged_df.dropna().drop_duplicates(subset=['track_name'])
    merged_df = merged_df[['id', 'artist_id', 'track_name', 'popularity', 'album_id',
       'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
       'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo','duration_ms',
       'time_signature']]
    merged_df['artist_id_encoded'] = le.fit_transform(merged_df['artist_id'])
    merged_df = merged_df.reset_index()
    return merged_df

def find_recommendation(name, input_df, df):
    feat = ['danceability', 'energy', 'key', 'loudness', 'speechiness','acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']
    X = df[feat]
    input = input_df[feat]
    X = pd.DataFrame(scaler.fit_transform(X), columns = feat)
    input = pd.DataFrame(scaler.transform(input), columns = feat)
    # add weight for same artist
    X['artist_id_encoded'] = df['artist_id_encoded'] * 100
    input['artist_id_encoded'] = input_df['artist_id_encoded'] * 100
    kmeans = KMeans(n_clusters=X.shape[0]//15, random_state=42)
    kmeans.fit(X)
    labels = kmeans.predict(X)
    df['labels'] = labels
    label = statistics.mode(kmeans.predict(input))
    input_features = input.values.flatten()
    similar_songs = df[df['labels'] == label]
    similar_songs = list(filter(lambda x:x not in name, list(similar_songs["track_name"])))
    return similar_songs

def main(track_names):
    df = preprocess(track,audio)
    df['input'] = df['track_name'].apply(lambda x:x in track_names)
    choice = df[df['input']==True].reset_index()
    songs = find_recommendation(track_names, choice, df)
    return songs