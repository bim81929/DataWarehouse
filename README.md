# DataWarehouse
## Chi tiết hệ thống
- Master (máy thật, ubuntu 20.04)
- 2 worker (máy ảo, 2 core, 3gb ram, ubuntu 20.04)
## Các yêu cầu
- Python 3.10
- Java 1.8
- Apache Hadoop (/usr/local/hadoop)
- Apache Spark (/usr/local/spark)

## Hướng dẫn chạy
### Môi trường Ubuntu/Debian
- sudo apt install libssl-dev libncurses5-dev libsqlite3-dev libreadline-dev libtk8.6 libgdm-dev libdb4o-cil-dev libpcap-dev libpq-dev libbz2-dev
- Cài java 1.8: sudo apt-get install openjdk-8-jdk
- Cài python 3.10: https://www.python.org/downloads/release/python-3105/
  - cd đến directory chứa python
  - ./configure --enable-optimizations
  - sudo make
  - sudo make install
### Cài thư viện
- pip install -r requirements.txt
### Master
- ``celery --app=common.celery.app --broker=redis://localhost:6379/0 --result-backend=redis://localhost:6379/1 flower --port=5555 --address=0.0.0.0 --loglevel=WARNING``
### Worker
- ``celery --app=common.celery.app --broker=redis://localhost:6379/0 --result-backend=redis://localhost:6379/1 worker --prefetch-multiplier=1 --concurrency=1 --pool=solo -Ofair``