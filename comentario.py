import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]

    # Crear comentario con UUID
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)

    # Guardar en S3 como JSON (estrategia Ingesta Push)
    s3 = boto3.client('s3')
    timestamp = datetime.utcnow().isoformat()
    nombre_archivo = f"{tenant_id}/{uuidv1}_{timestamp}.json"
    s3.put_object(
        Bucket=nombre_bucket,
        Key=nombre_archivo,
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    # Salida (json)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response
    }
