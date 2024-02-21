# ETL with Vehicle Toll Data DAG - Apache Airflow Project

This project is dedicated to the implementation of an Extract, Transform, Load (ETL) pipeline utilizing Apache Airflow. The primary objective is to streamline the processing of toll data collected from various sources, ensuring its transformation into a structured format and subsequent consolidation for analysis and storage purposes.

## Detailed Overview

The Apache Airflow Directed Acyclic Graph (DAG) named `ETL_toll_data` is the cornerstone of this project, orchestrating the intricate series of tasks required for efficient data processing. Below is a detailed breakdown of each component:

### DAG Arguments Specification

To ensure the seamless execution and management of the workflow, meticulous attention has been given to defining the DAG arguments:

- **Owner:** A placeholder name is assigned to denote ownership of the workflow.
- **Start Date:** Set to the current date, ensuring the immediate initiation of the DAG.
- **Email Configuration:** Dummy email addresses are configured for receiving notifications.
  - **Email on Failure:** Enabled to promptly notify stakeholders in case of task failures.
  - **Email on Retry:** Enabled to facilitate timely updates during task retries.
- **Retries:** Set to 1 to enable task retries in case of transient failures.
- **Retry Delay:** Configured with a delay of 5 minutes between retries, mitigating potential resource contention issues.

### Task Definitions and Operations

The DAG encompasses a meticulously crafted set of tasks, each tasked with specific data processing operations:

#### Unzip Data Task

The `unzip_data` task is responsible for decompressing downloaded data files into the designated destination directory. This operation is crucial as it prepares the raw data for subsequent processing stages.

#### Extract Data from CSV Task

Under the purview of the `extract_data_from_csv` task, targeted fields are extracted from a CSV file (`vehicle-data.csv`). The extracted data is then persisted into another CSV file (`csv_data.csv`), ensuring seamless data transformation.

#### Extract Data from TSV Task

In the `extract_data_from_tsv` task, predefined fields are extracted from a TSV file (`tollplaza-data.tsv`). This extracted data is subsequently stored in a CSV file (`tsv_data.csv`), facilitating efficient data manipulation and analysis.

#### Extract Data from Fixed Width Task

The `extract_data_from_fixed_width` task is entrusted with extracting specified fields from a fixed-width file (`payment-data.txt`). The extracted data is then formatted and saved into a CSV file (`fixed_width_data.csv`), ensuring consistency across data sources.

#### Consolidate Data Task

The `consolidate_data` task serves as the linchpin in the data processing pipeline. It amalgamates data from previously extracted CSV files (`csv_data.csv`, `tsv_data.csv`, `fixed_width_data.csv`) into a unified file (`extracted_data.csv`). Leveraging the `paste` command in a Bash operator, this task seamlessly merges disparate datasets, laying the groundwork for comprehensive analysis.

#### Transform Data Task

In the final leg of the pipeline, the `transform_data` task undertakes the transformation of the `vehicle_type` field within the consolidated dataset (`extracted_data.csv`). Employing capitalization as the transformation mechanism, this task ensures data uniformity and coherence. The transformed data is then stored in a new CSV file (`transformed_data.csv`) within the staging directory, ready for further processing or analysis.

### Task Pipeline Configuration

The task pipeline has been meticulously configured to orchestrate the sequence of operations, ensuring optimal workflow execution:

- **Unzip Data Task:** Initiates the data processing pipeline by decompressing downloaded files.
- **Extract Data Tasks:** Sequentially extracts data from CSV, TSV, and fixed-width files, ensuring a systematic approach to data extraction.
- **Consolidate Data Task:** Serves as the nexus for consolidating extracted data into a cohesive dataset, laying the foundation for comprehensive analysis.
- **Transform Data Task:** Culminates the pipeline by transforming the consolidated dataset, facilitating data uniformity and coherence.

## Conclusion

This project exemplifies the efficacy of Apache Airflow in automating complex data processing workflows. By meticulously defining tasks, configuring dependencies, and orchestrating operations, the ETL pipeline seamlessly transforms raw toll data into a structured format, ready for analysis and decision-making. With scalability and robustness at its core, this project paves the way for enhanced data-driven insights and operational efficiency.
