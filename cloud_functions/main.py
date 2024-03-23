import base64
from google.cloud import pubsub_v1
import json
from flask import request
from urllib.parse import urlparse
import logging

# Configurar el nivel de log
logging.basicConfig(level=logging.INFO)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('project_id', 'woocommerce_topic')

def extract_store_name(domain):
    # Lista de subdominios comunes que deseamos omitir
    ignore_subdomains = {'www'}

    # Dividir el dominio en partes
    parts = domain.split('.')

    # Filtrar las partes para eliminar subdominios comunes
    filtered_parts = [part for part in parts if part not in ignore_subdomains]

    # Devolver la primera parte del dominio filtrado
    return filtered_parts[0] if filtered_parts else ''

def webhook_to_pubsub(request):
    request_json = request.get_json()
    logging.info(f"Variable request_json: {request_json}")

    if request_json is not None:
        data = json.dumps(request_json).encode('utf-8')

        # Obtener encabezados HTTP
        headers = request.headers

        # Extraer el dominio del encabezado 'X-Wc-Webhook-Source' y obtener solo la primera parte del dominio
        webhook_source_url = headers.get('X-Wc-Webhook-Source', '')
        logging.info(f"Variable webhook_source_url: {webhook_source_url}")

        domain = urlparse(webhook_source_url).netloc
        store_name = extract_store_name(domain)

        # Utilizar el nombre de la tienda como el valor del atributo 'account'
        account_value = store_name
        logging.info(f"Variable account_value: {account_value}")
        
        # Publicar con un atributo personalizado
        future = publisher.publish(topic_path, data, account=account_value)
        future.result()  # Opcional, para confirmar la publicación

        return 'Message published to Pub/Sub', 200
    else:
        return 'Invalid request', 400
