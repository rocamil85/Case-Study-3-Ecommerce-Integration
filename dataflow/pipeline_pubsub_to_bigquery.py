#!/usr/bin/env python
# coding: utf-8

# In[1]:


import warnings
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
import logging

warnings.filterwarnings("ignore", category=DeprecationWarning)

def parse_message(pubsub_message):
    from datetime import datetime
    try:   
        message    = pubsub_message.data 
        attributes = pubsub_message.attributes 
        
        #logging.info("message: %s", message)
        #logging.info("attributes: %s", attributes)
                       
        store = attributes['X-Shopify-Shop-Domain']
        info = message.decode('utf-8')
        
        return {
            'platform': 'Shopify',
            'store': store,
            'date_info': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f UTC"),
            'info': info,
        }
    
    except Exception as e:
        logging.error("Unexpected error: %s", str(e))
        logging.error("Message causing error: %s", message.decode('utf-8'))
        return None

def filter_none(element):
    if element is None:
        logging.warning("Element is None and will be filtered out.")
        return False
    return True

def log_elements(element):
    logging.info("Element to be inserted in BigQuery: %s", str(element))
    return element


def run_pipeline():
    runner_type = 'DataflowRunner'  # Cambiar a 'DataflowRunner' para ejecución en GCP
    
    pipeline_options = PipelineOptions()
    
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.runner = runner_type
    standard_options.streaming = True 
    
    if runner_type == 'DataflowRunner':
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = 'project_id'
        google_cloud_options.job_name = 'job-streaming-dataflow2'
        google_cloud_options.staging_location = 'gs://ruta/bucket/temp'
        google_cloud_options.temp_location = 'gs://ruta/bucket/temp'
        google_cloud_options.region = 'us-central1'
               
        # Worker Options
        worker_options = pipeline_options.view_as(WorkerOptions)
        worker_options.machine_type = 'n1-standard-1'
        worker_options.max_num_workers = 3
        

    schema = {
        'fields': [
            {'name': 'platform', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'store', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date_info', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'info', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline 
            | 'Read from PubSub' >> ReadFromPubSub(subscription='projects/project_id/subscriptions/sub_poblado-historico_pull', with_attributes=True)
            | 'Parse and Process Message' >> beam.Map(parse_message)
            | 'Filter and Log None Messages' >> beam.Filter(filter_none)          
            | 'Log Elements Before Insertion' >> beam.Map(log_elements)  
            | 'Write to BigQuery' >> WriteToBigQuery(
                table='project_id:historical_data.historical',
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()


# In[ ]:





# In[ ]:




