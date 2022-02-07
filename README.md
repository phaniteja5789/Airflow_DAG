# Airflow_DAG

The repository contains the DAG and the output CSV File which contains the output of the PythonOperator used in the DAG

The WorkFlow of the DAG

![image](https://user-images.githubusercontent.com/36558484/152839527-bc6dcd87-2a47-423f-a0b0-8b0bfd80ea5b.png)


The DAG Contains 5 tasks
1.Creation of Table
2.Check whether the URL Endpoint is available
3.Get the Response from the URL
4.Convert the JSON Response into CSV format and fetch the related information
5.Store the CSV data into table

The DAG Contains SqliteOperator,HttpSensor,SimpleHttpOperator,PythonOperator,BashOperator

SqliteOperator-The Operator is used to create the table with the required columns
HttpSensor-The Operator is used to check whether the used URL EndPoint Exists or not
SimpleHttpOperator-The Operator is used to get the response from the URL Endpoint
PythonOperator-The Recieved response will be in Json format and the Python Operator Converts the recieved format into csv and converts it into dataframe and exports the result into csv file
BashOperator-The Bash Operator is used to insert the csv file data into table which is created using SQLiteOperator.
