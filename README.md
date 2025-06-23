
Weather ETL Pipeline using Apache Airflow, Python, and DuckDB
This project demonstrates the design and implementation of a modular ETL (Extract, Transform, Load) pipeline using Apache Airflow, targeting real-time weather data from the OpenWeatherMap API. The pipeline is structured into three distinct stages — extract, transform, and load — and orchestrated through a directed acyclic graph (DAG) in Airflow.
🔧 Key Components:
•	Extraction: A PythonOperator calls the OpenWeatherMap API to retrieve current weather data for Nairobi, then stores the raw JSON response using Airflow's XComs.
•	Transformation: The pipeline parses and structures relevant weather metrics (timestamp, city, temperature, humidity, weather description) into a clean Pandas DataFrame, which is also passed between tasks via XComs.
•	Loading: The final step loads the transformed data into a DuckDB table, ensuring persistent storage in a local database file. If the table doesn't exist, it's created automatically.
🚀 Technologies Used:
•	Airflow DAGs & PythonOperator for scheduling and orchestration
•	Pandas for data transformation
•	DuckDB for lightweight local data storage
•	XComs for inter-task communication in Airflow
•	OpenWeatherMap API as the live data source
This project exemplifies my ability to build a scalable, real-time data pipeline using modern tools and best practices in data engineering.

