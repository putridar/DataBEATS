from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from skopt import BayesSearchCV

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An Airflow DAG to train and predict a random forest model from multiple BigQuery tables',
    schedule_interval=timedelta(weeks=1)  # Run every week
)

def combine_data(**context):
    # Create a BigQueryHook
    # bq_hook = context['ti'].hook
    bq_hook = BigQueryHook()
    bq_hook.hook_name = str(context['ti'])

    # Define the SQL query to join the tables and retrieve the data
    sql_query = """
    SELECT *
    FROM Spotify.Tracks
    """

    # Execute the SQL query and retrieve the data as a Pandas DataFrame
    data = pd.read_gbq(query=sql_query, project_id=bq_hook.project_id, dialect='standard', credentials=bq_hook.get_credentials())

    # Store the data in XCom for use in later tasks
    context['ti'].xcom_push(key='data', value=data.to_dict())

def train_predict_model(**context):
    # Read data from context
    data = pd.DataFrame(context['ti'].xcom_pull(task_ids='combine_data', key='data'))
    data = data.drop(['artist_id', 'analysis_url', 'track_id', 'track_name', 'album_id'], axis=1)
    X = data.drop('popularity', axis=1)
    y = data['popularity']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    param_grid = {
    'bootstrap': [True],
    'max_depth': [80, 90, 100, 110],
    'max_features': [2, 3],
    'min_samples_leaf': [3, 4, 5],
    'min_samples_split': [8, 10, 12],
    'n_estimators': [100, 200, 300, 1000]
    }

    # Create a random forest model
    model = RandomForestRegressor()

    # Use Bayesian optimization to tune the hyperparameters
    bayes_cv_tuner = BayesSearchCV(
        estimator=model,
        search_spaces=param_grid,
        cv=3,
        n_jobs=-1,
        n_iter=10,
        verbose=0,
        random_state=42
    )
    bayes_cv_tuner.fit(X_train, y_train)

    # Print the best hyperparameters found by the Bayesian optimizer
    print("Best hyperparameters: ", bayes_cv_tuner.best_params_)

    # Use the best hyperparameters to train the final model
    model.set_params(**bayes_cv_tuner.best_params_)
    model.fit(X, y)

    # Save trained model to file
    joblib.dump(model, 'prediction_model.joblib')

get_data = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data,
    provide_context=True,
    dag=dag
)

predict_task = PythonOperator(
    task_id='train_predict_model',
    python_callable=train_predict_model,
    provide_context=True,
    dag=dag
)

get_data >> predict_task
