import gradio as gr
import numpy as np
import matplotlib.pyplot as plt
import joblib
from google.cloud import bigquery
from recommendation import find_recommendation, format_recommendation


# Code for the Dashboard page
def dashboard():
    x = np.linspace(-10, 10, 100)
    y = np.sin(x)
    fig, ax = plt.subplots()
    ax.plot(x, y)
    plt.title("Sinusoidal Curve")
    plt.xlabel("X-axis")
    plt.ylabel("Y-axis")
    plt.tight_layout()
    return fig


client = bigquery.Client.from_service_account_json("is3107-381408-7e3720e3fc1b.json")

# Load the pre-trained model
model = joblib.load("prediction_model.joblib")

QUERY = "SELECT * FROM Spotify.Audio_Features JOIN Spotify.Tracks ON Audio_Features.id = Tracks.track_id"


def get_tracks(QUERY):
    query_job = client.query(QUERY)
    query_result = query_job.result()
    data = query_result.to_dataframe()
    return data


tracks_data = get_tracks(QUERY)


# Code for the Model page
def model_prediction(item):
    # predict popularity
    data = tracks_data[tracks_data["track_name"] == item]
    data = data.drop(
        [
            "artist_id",
            "type",
            "id",
            "uri",
            "track_href",
            "analysis_url",
            "track_id",
            "track_name",
            "album_id",
        ],
        axis=1,
    )
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
            dropdown = gr.Dropdown(dropdown_options, label="Songs")
            get_dashboard = gr.Button("Generate Dashboard", variant="primary")
            plot = gr.Plot(dashboard())
            get_dashboard.click(fn=lambda: plot.update(dashboard()))

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
