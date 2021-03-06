# NanoDegree_DataLake
<h2> Summary </h2>
<p>This project is a data lake implementation of a music streaming data using Pyspark. The data resides in S3 as 2 different types; song and log data in json format.</p>
<p>etl.py file reads those data from s3 (Extract) and processes it to create 5 dimensional tables (Transform) and writes them back to s3 as parquet format.</p>

<h2> How to run the script </h2>
The job can be run using AWS's EMR platform where the cluster can be easily created to perform distributed computing using Spark.
Once the cluster is awake, you can follow the steps to run the etl.py using AWS EMR service:
<p>![Step1](https://github.com/erdemah/NanoDegree_DataLake/blob/master/images/addstep0.png)</p>
<p>![Step 2](https://github.com/erdemah/NanoDegree_DataLake/blob/master/images/add_step3.png)</p>

You can also run the spark job by connecting to the EMR's master node and run the following command using terminal:
spark-submit etl.py

<h3>Notes</h3>
The data resides in S3. The copy of the data is added to the repository inside log-data and song_data folders.
