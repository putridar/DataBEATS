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

Move the `dag.py`, `ml_training_dag.py`, and `config.py` to dags folder in airflow<br>

To test the DAG, run:<br>
`airflow dags test is3107_spotify_dag <DATE>`<br>
`airflow dags test is3107_ml_dag <DATE>`

Replace `<USER_DETAIL>` and `<DATE>` with the appropriate values.

## Running the App

Download the BigQuery API file and put it in the same folder as `app.py`<br>

To run the app, type the following command in your command prompt:<br>
`python app.py`

### Dashboard
