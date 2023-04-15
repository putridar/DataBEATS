import gradio as gr
import numpy as np
import matplotlib.pyplot as plt


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


# Code for the Model page
def model_prediction(item):
    # Replace this with your actual model prediction code
    return 80, f"The model predicts that {item} is a good choice!"


# Dropdown options for the Model page
dropdown_options = ["Item 1", "Item 2", "Item 3"]

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
      <p style='font-size: 38px; font-weight: bold; color: #ffffff'>Predict Condo Prices ft. Review Analytics</p>\
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
            recommendation = gr.Textbox(label="Recommendation")
            get_prediction.click(
                fn=model_prediction,
                inputs=[dropdown],
                outputs=[popularity, recommendation],
            )

demo.launch(debug=True, show_api=False)
