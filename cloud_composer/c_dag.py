import datetime
import time
import logging
import json
from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.operators.python import get_current_context


def cleanup():
    context = get_current_context()
    Variable.delete("pass_on_ctx_"+str(context['dag'].dag_id))
        

def example_success_call_back(context):
    # OTEL imports
    from opentelemetry import trace,metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics import MeterProvider

    # Obtain New Relic license key set as airflow envrionment variable 
    # This should be set in env vars as AIRFLOW_VAR_NEW_RELIC_API_KEY -> https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
    NEW_RELIC_API_KEY = Variable.get("NEW_RELIC_API_KEY")
    headers="api-key={}".format(NEW_RELIC_API_KEY)
    # Set New Relic OTLP endpoint
    endpoint="https://otlp.nr-data.net:4318"
    # Set name to use as New Relic entity
    resource = Resource(attributes={SERVICE_NAME: "Cloud Composer"})
    
    # Set OTEL tracer 
    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter(headers=headers, endpoint=endpoint))
    tracer = TracerProvider(resource=resource)
    tracer.add_span_processor(processor)
    tracer = trace.get_tracer(__name__, tracer_provider=tracer)
    
    # Set OTEL logger 
    LoggingInstrumentor().instrument(set_logging_format=True,log_level=logging.DEBUG)
    exporter = OTLPLogExporter(headers=headers, endpoint=endpoint)
    logger = logging.getLogger(str("dag_logger"))
    logger.handlers.clear()
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    logger.addHandler(handler)
    
    # Set OTEL meter 
    reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=endpoint,headers=headers))
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    meter = metrics.get_meter(__name__,meter_provider=provider)
    cloud_composer_example_metric_counter=meter.create_counter("cloud_composer_example_metric_counter")
    cloud_composer_example_metric_counter.add(1,attributes={"name":str(context['dag'].dag_id)})

    try:
        # Check if this is the first task to run, if this the first then create DAG parent span
        try:
            pass_on_ctx=Variable.get("pass_on_ctx_"+str(context['dag'].dag_id))
        except:
            pass_on_ctx=""
            Variable.set("pass_on_ctx_"+str(context['dag'].dag_id),pass_on_ctx)
        
        if pass_on_ctx != "":
            carrier = json.loads(pass_on_ctx.replace("\'", "\""))
            ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
        else:
            dag_conf = context['dag_run'].conf
            traceparent= dag_conf["traceparent"]
            carrier = {'traceparent': traceparent}
            ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
            start_time=dt_to_ns_epoch(context['execution_date'])
            parent_span=tracer.start_span(name="DAG: "+context['dag'].dag_id,context=ctx,start_time=start_time,kind=trace.SpanKind.SERVER)
            with trace.use_span(parent_span, end_on_exit=False):
                parent_span.set_attribute("dag_id",context['dag_run'].dag_id)
                parent_span.set_attributes(carrier)
                ctx=trace.set_span_in_context(parent_span)
                
        start_time=dt_to_ns_epoch(context['task_instance'].start_date)
        span=tracer.start_span(name="DAG: "+context['dag'].dag_id+" TASK: "+context['task'].task_id,context=ctx,start_time=start_time,kind=trace.SpanKind.SERVER)
        with trace.use_span(span, end_on_exit=False):
            span.set_attribute("dag_id",context['dag_run'].dag_id)
            span.set_attributes(carrier)
            end_time=dt_to_ns_epoch(context['task_instance'].end_date)
            try:
                pass_on_carrier = {}
                # Write the current context into the carrier.
                TraceContextTextMapPropagator().inject(pass_on_carrier)
                Variable.update("pass_on_ctx_"+str(context['dag'].dag_id),pass_on_carrier)
            except Exception as e:
                logger.error(str(e))
        span.end(end_time=end_time)
        if pass_on_ctx == "":
            parent_span.end()
        provider.force_flush()
    except Exception as e:
        logger.error(str(e))
    time.sleep(5)

def dt_to_ns_epoch(dt):
    return int(dt.timestamp() * 1000000000)


with models.DAG(
    "c_dag",
    start_date=datetime.datetime(2021, 1, 1),
    # Not scheduled, trigger only
    schedule=None,
    
) as dag:
    def greeting():
        logging.info("Hello World!")
        time.sleep(5)
    
    hello_python = PythonOperator(task_id="hello", python_callable=greeting,on_success_callback=example_success_call_back)

    goodbye_bash = BashOperator(task_id="bye", bash_command="echo Goodbye {{ var.value.get('pass_on_ctx_c_dag', 'FAILURE') }}",on_success_callback=example_success_call_back)
    
    trigger_dependent_dag = TriggerDagRunOperator(task_id="trigger_dependent_dag",wait_for_completion=False,trigger_dag_id="c_dataproc_instantiate",conf={"traceparent":"{{ var.value.get('pass_on_ctx_c_dag', 'FAILURE') }}"},on_success_callback=example_success_call_back)

    cleanup_task = PythonOperator(task_id="cleanup", python_callable=cleanup)
    
    hello_python >> goodbye_bash >> trigger_dependent_dag >> cleanup_task

