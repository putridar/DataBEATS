import pandas as pd
import joblib
from sklearn.cluster import KMeans
import statistics
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler

le = LabelEncoder()
scaler = StandardScaler()


def preprocess(df):
    merged_df = df.dropna().drop_duplicates(subset=["track_name"])
    merged_df = merged_df[
        [
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
            "duration_ms",
            "time_signature",
        ]
    ]
    merged_df["artist_id_encoded"] = le.fit_transform(merged_df["artist_id"])
    merged_df = merged_df.reset_index()
    return merged_df


def cluster(name, input_df, df):
    feat = [
        "danceability",
        "energy",
        "key",
        "loudness",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo",
    ]
    X = df[feat]
    input = input_df[feat]
    X = pd.DataFrame(scaler.fit_transform(X), columns=feat)
    input = pd.DataFrame(scaler.transform(input), columns=feat)
    # add weight for same artist
    X["artist_id_encoded"] = df["artist_id_encoded"] * 100
    input["artist_id_encoded"] = input_df["artist_id_encoded"] * 100
    kmeans = KMeans(n_clusters=X.shape[0] // 15, random_state=42)
    kmeans.fit(X)
    labels = kmeans.predict(X)
    df["labels"] = labels
    label = statistics.mode(kmeans.predict(input))
    similar_songs = df[df["labels"] == label]
    similar_songs = list(
        filter(lambda x: x not in name, list(similar_songs["track_name"]))
    )
    return similar_songs


def find_recommendation(df, track_names):
    df = preprocess(df)
    df["input"] = df["track_name"].apply(lambda x: x in track_names)
    choice = df[df["input"] == True].reset_index()
    songs = cluster(track_names, choice, df)
    return songs


def format_recommendation(lst):
    string = f"{1}. {lst[0]}" if lst else ""
    count = 2
    for song in lst:
        string += f"\n{count}. {song}"
        count += 1
    return string
