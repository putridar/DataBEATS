# DataBeats

## Setup

To set up the project, first install the required dependencies by running:<br>
`pip install -r requirements.txt`

Next, initialize the Airflow database by running:<br>
`airflow db init`

Create a new user by running:<br>
`airflow create user <USER_DETAIL>`

Then, start the Airflow webserver on port 8080 with:<br>
`airflow webserver --port 8080`

Start the Airflow scheduler with:<br>
`airflow scheduler`

To test the DAG, run:<br>
`airflow dags test is3107_spotify_dag <DATE>`

Replace `<USER_DETAIL>` and `<DATE>` with the appropriate values.

## Running the App

To run the app, type the following command in your command prompt:<br>
`python app.py`

### Dashboard

The app includes a dashboard that allows you to analyze Spotify tracks.

### Machine Learning

Choose a song, and the app will provide a popularity prediction and song recommendation based on machine learning models.
