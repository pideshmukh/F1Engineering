❗Objective:
Designed and implemented a scalable, end-to-end data engineering solution on the Microsoft Azure platform to process and analyze historical Formula 1 racing data. The project involved building a robust data lakehouse architecture to provide actionable insights into driver and team performance.

⚙️ Azure Databricks:
Databricks is used for processing large volumes of data, in which only the Data Factory would not be able to perform the appropriate processing. It has a range of possibilities, through the spark framework, and can use integrations and communications between the Spark SQL, PySpark, Scala and R languages.

⚙️ Azure Data Factory:
Microsoft's main cloud tool used for data collection, ingestion and orchestration tasks.
Debug operations, scheduling by Event Trigger, Schedule Trigger and Tumbling Window Trigger.
Integration and orchestration with Databricks, Orchestration of all activites via pipeline.

📂 Azure Data Lake Storage Gen2:
Microsoft cloud tools used for file storage focused on Big Data, access control and information centralization.
Gen2 Data Lake Creation, Conainers, Data Upload, IAM.
Creating Workspace in Azure Databricks, creating Clusters, performing mount operations in Storage Account, creating notebooks, transformations via pyspark, requesting notebooks via ADF.

📄 Structuring of project:
The project consists of obtaining Formula 1 data via the Ergast API and storing the data in a raw layer. After that, pipelines and code are created for a processing layer as well as an analysis layer. The entire process is orchestrated via ADF, and, at the end of the entire data mat, it is possible to perform analyses in Databricks itself and/or using Power BI.

📦 Data sources:
The sources of the data are from the Ergast containing information on teams, years, drivers, among others.

💻 Proposed architecture:
<img width="1256" height="692" alt="image" src="https://github.com/user-attachments/assets/0065e6c5-5923-4563-bc6d-50b03376060c" />
