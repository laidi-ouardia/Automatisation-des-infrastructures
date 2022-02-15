import pandas as pd
import os
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable

DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

@dag(DAG_NAME,  schedule_interval="0 0 * * *", start_date=days_ago(1))
def dag_projet():
    """
    Ce DAG est pour projet automatisation
    """
    @task()
    def test():
        trip2 = pd.read_csv("table_resultante.csv", sep=",", error_bad_lines=False)
        print(table_resultante.head())
    test()
dag_groupe16_instance =dag_projet() #Instanciation
