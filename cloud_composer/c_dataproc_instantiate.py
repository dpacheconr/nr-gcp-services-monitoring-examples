"""Example Airflow DAG that kicks off a Cloud Dataproc Template that runs a
Spark Pi Job.

This DAG relies on an Airflow variable
https://airflow.apache.org/docs/apache-airflow/stable/concepts/variables.html
* project_id - Google Cloud Project ID to use for the Cloud Dataproc Template.
"""

import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocInstantiateWorkflowTemplateOperator,
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

import datetime
import time
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json

# This DAG is not running any call backs, it's simply passing trace context to dataproc

def cleanup():
    Variable.delete("traceparent")
    
project_id="PROJECTNAME"
pass_on_ctx ="{{ dag_run.conf['traceparent']}}"

default_args = {
    "project_id": project_id,
    "pass_on_ctx": pass_on_ctx,
}
        
with models.DAG(
    "c_dataproc_instantiate",
    start_date=datetime.datetime(2021, 1, 1),
    schedule=None,
    tags=["db-poc"],
    default_args=default_args,  
    
) as dag:
    def parse_variables(**kwargs):
        traceparent_json = json.loads(kwargs['pass_on_ctx'].replace("\'", "\""))
        traceparent=traceparent_json['traceparent']
        Variable.set("traceparent",traceparent)
        
    parse_variable = PythonOperator(task_id="parse_variables", python_callable=parse_variables,op_kwargs={'pass_on_ctx': pass_on_ctx},)
    start_template_job = DataprocInstantiateWorkflowTemplateOperator(task_id="dataproc_workflow_dag",template_id="scalajob",project_id=project_id,region="us-central1", parameters={"TRACEPARENT": "{{ var.value.get('traceparent', 'null') }}"})
    cleanup_task = PythonOperator(task_id="cleanup", python_callable=cleanup)
    
parse_variable >> start_template_job >> cleanup_task