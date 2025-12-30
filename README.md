#

```python -m venv .venv```

В Minio создаем bucket "Create Bucket" (ex:"supfun")
Создаем там ключи Create access key -> Create.
Копируем Access and  Secret Key. Записываем их в отдельной папке в корне нашего проекта, также этот файл необходимо скрывать

Затем эти ключи скрываем в Airflow.
Admin -> Variable: key: "access_key"
                   val: "your_access_key"
-> Save. Также для Secret key название(любое) и сам ключ.
Также в Variable создаем. Key: pg_password
                          Val: postgres

Затем также в Airflow создаем соединение Admin -> Connections -> add new connection
Conection ID: postgres_dwh
Connection Type: Postgres
Host: postgres_dwh(то что мы созавали в docker-compose)
Database: postgres
Login: postgres
Password: postgres
Port: 5432
-> Save