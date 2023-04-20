import gradio as gr
import numpy as np
import matplotlib.pyplot as plt
import joblib
from google.cloud import bigquery
from recommendation import find_recommendation, format_recommendation

client = bigquery.Client.from_service_account_json("is3107-381408-7e3720e3fc1b.json")


# Code for the Dashboard page
def get_artist_df():
    query_artist = "SELECT * FROM Spotify.Artists"
    query_job = client.query(query_artist)
    query_result = query_job.result()
    data = query_result.to_dataframe()
    return data


def get_tracks_df():
    query_track = "SELECT * FROM Spotify.Tracks"
    query_job = client.query(query_track)
    query_result = query_job.result()
    data = query_result.to_dataframe()
    return data


def get_albums_df():
    query_album = "SELECT * FROM Spotify.Albums"
    query_job = client.query(query_album)
    query_result = query_job.result()
    data = query_result.to_dataframe()
    return data


def dashboard(insightType):
    fig = plt.figure(figsize=(10, 5))

    if insightType == "Tracks Popularity":
        df_spotify = get_tracks_df()
        df2 = df_spotify.sort_values(by=["popularity"], ascending=False)
        df2 = df2.dropna().drop_duplicates(subset=["track_name"])
        plt.barh(df2["track_name"][:20], df2["popularity"][:20], color="maroon")
        plt.xlabel("Popularity")
        plt.ylabel("Track Name")
        plt.title("Top 20 Popular Songs")
        plt.gca().invert_yaxis()
        plt.show()

    elif insightType == "Artists Popularity":
        df_spotify = get_artist_df()
        df_stream = df_spotify.sort_values(by=["popularity"], ascending=False)
        df_stream = df_stream.dropna().drop_duplicates(subset=["artist_name"])
        plt.barh(
            df_stream["artist_name"][:20], df_stream["popularity"][:20], color="maroon"
        )
        plt.xlabel("Popularity")
        plt.ylabel("Artist Name")
        plt.title("Top 20 Popular Artist")
        plt.gca().invert_yaxis()
        plt.show()

    """
    elif (insightType == 'Album Popularity'):
      df_weeksOnChart = df_spotify.sort_values(by=["weeks_on_chart"], ascending=False)
      plt.barh(df_weeksOnChart["track_name"][:20], df_weeksOnChart["weeks_on_chart"][:20], color='maroon')
      plt.xlabel("Length of Weeks on Chart")
      plt.ylabel("Track Name")
      plt.title("Top 50 Reigning Songs")
      plt.gca().invert_yaxis()
      plt.show()
    """

    return fig


# Load the pre-trained model
model = joblib.load("prediction_model.joblib")


tracks_data = get_tracks_df()


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
    songs = format_recommendation(find_recommendation(tracks_data, item))
    return output[0], songs


# Dropdown options for the Model page
dropdown_options = list(set(tracks_data["track_name"]))

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
            insightType = gr.Dropdown(
                ["Tracks Popularity", "Artists Popularity"], label="Choice"
            )
            get_dashboard = gr.Button("Generate Dashboard", variant="primary")
            output = gr.Plot()
            get_dashboard.click(fn=dashboard, inputs=insightType, outputs=output)

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

demo.launch(debug=True, show_api=False)
