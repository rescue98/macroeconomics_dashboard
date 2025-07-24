# Macroeconomics Dashboard

This project is a comprehensive dashboard designed to visualize and analyze key macroeconomic indicators. It offers interactive charts, data tables, and customizable views to help users understand economic trends and make informed decisions. The dashboard is user-friendly and supports the integration of various datasets relevant to macroeconomic analysis.

## Architecture Overview

The system is divided into several main components:

- **Data Ingestion**: Data is collected from multiple sources and ingested using scheduled DAGs (Directed Acyclic Graphs) managed by Apache Airflow.
- **Data Processing**: The ingested data is processed and transformed using Spark jobs, which ensure scalability and efficient handling of large datasets.
- **Dashboard Application**: The processed data is served to the dashboard frontend, where users can interact with visualizations and tables.

Below is a simplified diagram of the architecture:

```
+-------------------+      +-------------------+      +----------------------+
|   Data Sources    | ---> |   Airflow DAGs    | ---> |   Spark Processing   |
+-------------------+      +-------------------+      +----------------------+
                                                           |
                                                           v
                                                +----------------------+
                                                |   Dashboard Frontend |
                                                +----------------------+
```

- **Airflow DAGs** orchestrate the data pipelines, scheduling and monitoring data ingestion and processing tasks.
- **Spark jobs** handle data transformation and aggregation, preparing datasets for visualization.
- The **dashboard frontend** provides interactive tools for users to explore macroeconomic data.

This modular architecture allows for easy maintenance, scalability, and customization to meet different analytical needs.