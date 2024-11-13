we used DataBricks to create multiple ETL pipelines using the Python API of Apache Spark i.e. PySpark.

We have used sources like CSV, Parquet, and Delta Table then used Factory Pattern to create the reader class. Factory Pattern is one of the most used Low-Level designs in Data Engineering pipelines that involve multiple sources.

Then we used PySpark DataFrame API and Spark SQL to write the business transformation logic. In the loader part, we have loaded data into two fashion one using DataLake and another by Data LakeHouse

Bussiness Logic 

1 Customer who have bought Airpods after iphone

2 Customers who have bought both Airpods and iphone

3 List all the products bought by customers after their initial purchase