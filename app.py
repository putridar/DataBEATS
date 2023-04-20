import gradio as gr
import numpy as np
import matplotlib.pyplot as plt
import joblib
from google.cloud import bigquery
import pandas as pd
from recommendation import find_recommendation, format_recommendation

client = bigquery.Client.from_service_account_json("is3107-381408-7e3720e3fc1b.json")

def get_artist_df():
    query_artist = "SELECT * FROM Spotify.Artists"
    query_job = client.query(query_artist)
    query_result = query_job.result()
    data = query_result.to_dataframe()
    data = data.dropna().drop_duplicates(subset=["artist_name"])
    return data

def get_tracks_df():
    query_track = "SELECT * FROM Spotify.Tracks"
    query_job = client.query(query_track)
    query_result = query_job.result()
    data = query_result.to_dataframe()
    data = data.dropna().drop_duplicates(subset=["track_name"])
    return data

def get_albums_df():
    query_album = "SELECT * FROM Spotify.Albums"
    query_job = client.query(query_album)
    query_result = query_job.result()
    data = query_result.to_dataframe()
    data = data.dropna().drop_duplicates(subset=["album_name"])
    return data

def get_features_artist(artist1, artist2, feature):
    artists = get_artist_df()
    artist_1_id = artists[artists['artist_name']==artist1]['artist_id'].item()
    artist_2_id = artists[artists['artist_name']==artist2]['artist_id'].item()
    tracks = get_tracks_df()
    tracks_filtered = tracks[(tracks['artist_id'] == artist_1_id) | (tracks['artist_id'] == artist_2_id)]
    final_df = tracks_filtered[['artist_id', feature]]
    final_df['artist_name'] = final_df['artist_id'].apply(lambda x : artist1 if x == artist_1_id else artist2)
    return final_df

def get_features_album(album1, album2, feature):
    albums = get_albums_df()
    album_1_id = albums[albums['album_name']==album1]['id'].item()
    album_2_id = albums[albums['album_name']==album2]['id'].item()
    tracks = get_tracks_df()
    tracks_filtered = tracks[(tracks['album_id'] == album_1_id) | (tracks['album_id'] == album_2_id)]
    final_df = tracks_filtered[['album_id', feature]]
    final_df['album_name'] = final_df['album_id'].apply(lambda x : album1 if x == album_1_id else album2)
    return final_df

# Code for the Dashboard page
def dashboard(insightType):

    fig = plt.figure(figsize=(15, 5))

    if (insightType == 'Track Popularity'):
      df_spotify = get_tracks_df()
      df2 = df_spotify.sort_values(by=["popularity"], ascending=False)
      plt.barh(df2["track_name"][:20], df2["popularity"][:20], color='maroon')
      plt.xlabel("Popularity")
      plt.ylabel("Track Name")
      plt.title("Top 20 Popular Songs") 
      plt.gca().invert_yaxis()
      plt.show()

    elif (insightType == 'Artist Popularity'):
      df_spotify = get_artist_df()
      df_stream = df_spotify.sort_values(by=["popularity"], ascending=False)
      df_stream = df_stream.dropna().drop_duplicates(subset=["artist_name"])
      plt.barh(df_stream["artist_name"][:20], df_stream["popularity"][:20], color='maroon')
      plt.xlabel("Popularity")
      plt.ylabel("Artist Name")
      plt.title("Top 20 Popular Artist")
      plt.gca().invert_yaxis()
      plt.show()
    
    elif (insightType == 'Album Popularity'):
      df_album = get_album_df()
      df_album = df_album.sort_values(by=["popularity"], ascending=False)
      plt.barh(df_album["album_name"][:20], df_album["popularity"][:20], color='maroon')
      plt.xlabel("Popularity")
      plt.ylabel("Album Name")
      plt.title("Top 20 Popular Album")
      plt.gca().invert_yaxis()
      plt.show()
    
    return fig

def audio_dashboard(artist1, artist2, feature):
    df = get_features_artist(artist1, artist2, feature)
    fig = plt.figure(figsize=(15, 5))
    ax = pt.RainCloud(x = 'artist_name', y = feature, data = df, width_viol = .8, width_box = .4, orient = 'h')
    return fig

def weeksOnChart(entity):
    fig = plt.figure(figsize=(15, 5))
    df_chart = pd.DataFrame()
    name = ""
    if (entity == "Artist Popularity"):
      df_chart = get_artist_df()
      name = "artist_name"
    elif (entity == "Track Popularity"):
      df_chart = get_tracks_df()
      name = "track_name"
    elif (entity == "Album Popularity"):
      df_chart = get_albums_df()
      name = "album_name"
    df2 = df_chart.sort_values(by=["chart"], ascending=False)
    plt.barh(df2[name][:20], df2["chart"][:20], color='maroon')
    plt.xlabel("Weeks on Chart")
    plt.ylabel(name)
    plt.title("Top 20 Reigning Songs") 
    plt.gca().invert_yaxis()
    plt.show()
    return fig

def audio_feature_plot():
    model = joblib.load("prediction_model.joblib")
    feature_names = [
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
            "time_signature"
    ]
    forest_importances = pd.Series(model.feature_importances_, index=feature_names)
    fig, ax = plt.subplots()
    forest_importances.plot.bar(ax=ax)
    ax.set_title("Audio Feature Importance")
    ax.set_ylabel("Mean decrease in impurity")
    fig.tight_layout()
    return fig

tracks_data = get_tracks_df()
model = joblib.load("prediction_model.joblib")

# Code for the Model page
def model_prediction(item):
    # predict popularity
    data = tracks_data[tracks_data["track_name"] == item]
    print(data.columns)
    data = data[
        [
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
            "popularity",
        ]
    ]
    X = data.drop("popularity", axis=1)
    output = model.predict(X)
    # predict recommendation
    songs = find_recommendation(tracks_data, item)
    songs = format_recommendation(songs)
    return output[0], songs

# Dropdown options for the Model page
dropdown_options = list(set(tracks_data["track_name"]))
artist_list = get_artist_df()["artist_name"].values.tolist()
features_list = ["danceability", "energy", "liveness", "speechiness", "acousticness", "instrumentalness", "tempo"]
album_list = get_albums_df()["album_name"].values.tolist()

with gr.Blocks(
    css=".gradio-container {width: 100%} .tab-label {font-weight: bold, font-size: 16px}"
) as demo:
    with gr.Row():
        gr.Markdown("# DataBeats")
    gr.HTML(
        "<div>\
    <div style='position: absolute;top: 0;left: 0;width: 100%;height: 100%; z-index=1'></div>\
    <div style='position: absolute;top: 0;left: 0;width: 100%;height: 100%;background-color: rgba(255, 255, 255, 0.5); z-index: 1'></div>\
    <div style='position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center;z-index: 2'>\
      <p style='font-size: 38px; font-weight: bold; color: #ffffff'>Spotify Analysis</p>\
    </div>"
    )

    with gr.Tab("Dashboard", elem_classes="tab-label"):
        with gr.Column():
            insightType = gr.Dropdown(['Track Popularity', 'Artist Popularity', 'Album Popularity'], label="Choice")
            get_dashboard = gr.Button("Generate Dashboard", variant="primary")
            output = gr.Plot()
            get_dashboard.click(fn=dashboard, inputs=insightType, outputs=output)
        with gr.Column():
            get_chart = gr.Button("Generate Weeks On Chart", variant="primary")
            output = gr.Plot()
            get_chart.click(fn=weeksOnChart, inputs=insightType, outputs=output)
        with gr.Column():
            artist1 = gr.Dropdown(artist_list, label = "Artist Choice 1")
            artist2 = gr.Dropdown(artist_list, label = "Artist Choice 2")
            audio_feature = gr.Dropdown(features_list, label = "Audio Features")
            get_audio_dashboard = gr.Button("Generate Audio Dashboard", variant="primary")
            output = gr.Plot()
            get_audio_dashboard.click(fn=audio_dashboard, inputs=[artist1, artist2, audio_feature], outputs=output)
    with gr.Tab("ML", elem_classes="tab-label"):
        with gr.Column():
            dropdown = gr.Dropdown(dropdown_options, label="Songs")
            get_prediction = gr.Button("Predict", variant="primary")
        with gr.Column():
            popularity = gr.Textbox(label="Popularity")
            recommendation = gr.Textbox(label="Recommended Songs")
            get_prediction.click(
                fn=model_prediction,
                inputs=[dropdown],
                outputs=[popularity, recommendation],
            )
        with gr.Column():
            get_audio_features_importance = gr.Button("Generate Audio Feature Importances", variant="primary")
            output = gr.Plot()
            get_audio_features_importance.click(fn=audio_feature_plot, outputs=output)

demo.launch(debug=True, show_api=False)
