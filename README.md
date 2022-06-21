# DataWarehouse
## Các yêu cầu
- Python 3.10
- Java 1.8
- Apache Spark (/usr/local/spark)
- PosgreSql
- Redis
- Flower

## Hướng dẫn chạy
### Cài thư viện
- pip install -r requirements.txt
### Master
- ``celery --app=common.celery.app --broker=redis://localhost:6379/0 --result-backend=redis://localhost:6379/1 flower --port=5555 --address=0.0.0.0 --loglevel=WARNING``
### Worker
- ``celery --app=common.celery.app --broker=redis://localhost:6379/0 --result-backend=redis://localhost:6379/1 worker --prefetch-multiplier=1 --concurrency=1 --pool=solo -Ofair``