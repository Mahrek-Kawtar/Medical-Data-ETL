# Medical Data Processing and Migration Project

## Overview

This project involves extracting, transforming, and loading medical data into PostgreSQL and Supabase, executing SQL queries, and automating data migration.

---

## Code Repository Structure

### Python Scripts:
1. **`data_processing.ipynb`**: Data cleaning and transformation.
2. **`data_psycop2.py`**: Loading data into PostgreSQL.
3. **`migration_vers_supabase.py`**: Migrating data from PostgreSQL to Supabase.
4. **`data_supabase.py`**: A Python script for interacting with Supabase, including querying and updating data.
5. **`querying_filtering.py`**: Executing SQL queries on PostgreSQL.
6. **`test_queries.py`**: Testing SQL queries.

### SQL Scripts:
Included within Python scripts for table creation and data manipulation.

### CSV Files:
Sample medical data files.

### Supabase Directory:
Supabase configuration and setup files.

---

## Explanation of the Files Used

### **`data_processing.ipynb`**
- **Purpose**: This Jupyter notebook handles the data cleaning and transformation process. It includes steps for loading raw data, handling missing values, and performing transformations to prepare the data for insertion into PostgreSQL and Supabase.
- **Key Functions**: 
    - Normalizing column names.
    - Handling missing values.
    - Merging datasets.

### **`data_psycop2.py`**
- **Purpose**: This Python script loads the cleaned and transformed medical data into a local PostgreSQL database. It establishes a connection to the database and inserts the data into the appropriate tables.
- **Key Functions**:
    - Connecting to PostgreSQL using `psycopg2`.
    - Inserting data into the patients, visits, medications, and prescriptions tables.
    - Handling potential errors during the data insertion process.

### **`migration_vers_supabase.py`**
- **Purpose**: This script migrates data from PostgreSQL to Supabase. It connects to both PostgreSQL and Supabase databases, retrieves data from PostgreSQL, and inserts it into Supabase.
- **Key Functions**:
    - Connecting to both PostgreSQL and Supabase.
    - Transferring data from PostgreSQL to Supabase.

### **`data_supabase.py`**
- **Purpose**: This Python script interacts with Supabase, allowing the user to query and update data in Supabase. It performs actions such as retrieving data, inserting new records, and modifying existing records in the Supabase database.
- **Key Functions**:
    - Connecting to Supabase using the Supabase API.
    - Executing SQL queries on Supabase to retrieve or manipulate data.
    - This script is essential for managing data directly within the Supabase platform.

### **`querying_filtering.py`**
- **Purpose**: This script is designed to execute SQL queries on the PostgreSQL database to retrieve and filter data. It contains functions to:
    - Fetch patient visits.
    - Filter visits by diagnosis or date range.
    - Calculate statistics such as the number of visits per month.
    - Calculate the average visits per patient.
- **Example Functions**:
    - `get_visits_for_patient(patient_id)`
    - `filter_visits_by_diagnosis_or_date(diagnosis=None, start_date=None, end_date=None)`
    - `visits_per_month()`
    - `average_visits_per_patient()`

### **`test_queries.py`**
- **Purpose**: This script tests the SQL queries to ensure they return the expected results. It is used to validate that the filtering and querying logic works as intended.
- **Key Functions**:
    - Running predefined queries.
    - Checking the correctness of the data returned by the queries.

---

## Setup Instructions

### PostgreSQL Setup

1. **Start PostgreSQL**:
    ```bash
    sudo service postgresql start
    ```

2. **Check PostgreSQL status**:
    ```bash
    sudo service postgresql status
    ```

3. **Access PostgreSQL**:
    ```bash
    psql -h localhost -U postgres -d medical_records
    ```

4. **Install required dependencies**:
    ```bash
    pip install psycopg2
    pip install psycopg2-binary
    ```

5. **Run the data loading script**:
    ```bash
    python3 data_psycop2.py
    ```

6. **Open PostgreSQL shell**:
    ```bash
    psql -U postgres -d medical_records
    ```

---

### Supabase Setup

1. **Install Supabase CLI**:
    ```bash
    npm install supabase
    ```

2. **Start Supabase**:
    ```bash
    npx supabase start
    ```

---

## Automating Data Migration

To automate migration from PostgreSQL to Supabase every 3 hours:

1. Open crontab:
    ```bash
    crontab -e
    ```

2. Add the following line to schedule migration at 3 AM:
    ```bash
    0 3 * * * /usr/bin/python3 /mnt/c/test-data/data_supabase.py
    ```

3. Verify scheduled tasks:
    ```bash
    ls
    ```

---

## Differences Between PostgreSQL and Supabase

- **Configuration**: PostgreSQL runs locally on `localhost:5432`, while Supabase uses `localhost:54322`.
- **Authentication**: PostgreSQL requires manual setup, whereas Supabase provides a web-based authentication system.
- **Administration**: Supabase includes an integrated dashboard for managing tables, whereas PostgreSQL relies on command-line tools like `psql`.
- **Deployment**: Supabase enables cloud-based database hosting, whereas PostgreSQL is local.

---

## Testing and Validation

### Example Outputs

To test and validate the queries, run the `test_queries.py` script, which will print out results similar to the following examples.

1. **Example Query 1: Number of Visits per Patient**
    - SQL Query:
        ```sql
        SELECT patient_id, COUNT(*) AS visit_count
        FROM visits
        GROUP BY patient_id;
        ```
    - **Expected Output**:
        ```text
        patient_id | visit_count
        -----------|-------------
        101        | 5
        102        | 3
        103        | 7
        ```

2. **Example Query 2: Visits by Diagnosis**
    - SQL Query:
        ```sql
        SELECT diagnosis, COUNT(*) AS visits_count
        FROM visits
        GROUP BY diagnosis;
        ```
    - **Expected Output**:
        ```text
        diagnosis  | visits_count
        -----------|-------------
        Diabetes   | 15
        Hypertension | 10
        Flu        | 8
        ```

