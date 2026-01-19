# Practica_AWS_PaulaArnaiz

Comandos para el despliegue/teardown:

### 1) Deploy
cd .\infra
python .\deploy.py

### 2) Subir CSV
cd ..
aws s3 cp .\inventario.csv s3://inventory-uploads-<SUFFIX>/

### 3) Forzar stock bajo (crear item con Count=0)
@'
{
  "TableName": "Inventory",
  "Item": {
    "Store": {"S": "Berlin"},
    "Item":  {"S": "StockPrueba"},
    "Count": {"N": "0"}
  }
}
'@ | Set-Content -Encoding utf8 item-low.json

aws dynamodb put-item --cli-input-json fileb://item-low.json

### 4) Teardown
cd .\infra
python .\teardown.py
