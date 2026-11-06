# разработать DWH с бачевой обработкой для ритейл домена с технологиями Greenplum, Airflow, Spark


## Результат моей работы 
Дажборд, которыый принесет деньги бизнесу 
![alt text](image-3.png)

и рабочий пайплайн для этого дажборда 
![alt text](image-4.png)


## **Архитектура**  
![alt text](image.png)

Данные взяты с [Kaggle Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_orders_dataset.csv)


## to  start 

```bash
docker-compose up -d
```

connect user with minIo and get ACCESS_KEY and SECRET_ACCESS_KEY
```bash
mc.exe alias set <alias_name> https://localhost:9000 <ENV_ACCESS_KEY_ID> <ENV_SECRET_ACCESS_KEY>

mc.exe admin accesskey create <alias_name>
```


рефакторинг дага )  
![alt text](image-2.png)

