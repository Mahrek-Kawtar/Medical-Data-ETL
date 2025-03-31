import psycopg2
import pandas as pd


def connect_to_postgresql():
    try:
        conn = psycopg2.connect(
            dbname="medical_records",  
            user="postgres",           
            host="localhost",          
            port="5432"                
        )
        print("Connexion réussie à PostgreSQL")
        return conn
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return None

# Requête pour récupérer toutes les visites d'un patient donné
def get_visits_for_patient(patient_id):
    conn = connect_to_postgresql()
    if conn:
        query = f"SELECT * FROM visits WHERE patient_id = '{patient_id}';"
        df = pd.read_sql(query, conn)
        print(f"Visites pour le patient {patient_id}:")
        print(df)
        conn.close()


# Filtrer les visites en fonction du diagnostic ou des dates
def filter_visits_by_diagnosis_or_date(diagnosis=None, start_date=None, end_date=None):
    conn = connect_to_postgresql()
    if conn:
        query = "SELECT * FROM visits WHERE 1=1"
        
        if diagnosis:
            query += f" AND diagnosis = '{diagnosis}'"
        if start_date:
            query += f" AND visit_date >= '{start_date}'"
        if end_date:
            query += f" AND visit_date <= '{end_date}'"
        
        df = pd.read_sql(query, conn)
        print("Visites filtrées :")
        print(df)
        conn.close()
   
# Nombre de visites par mois
def visits_per_month():
    conn = connect_to_postgresql()
    if conn:
        query = """
        SELECT EXTRACT(MONTH FROM visit_date) AS month, COUNT(*) AS num_visits
        FROM visits
        GROUP BY month
        ORDER BY month;
        """
        df = pd.read_sql(query, conn)
        print("Nombre de visites par mois :")
        print(df)
        conn.close()

# Moyenne des visites par patient
def average_visits_per_patient():
    conn = connect_to_postgresql()
    if conn:
        query = """
        SELECT patient_id, AVG(visit_count) AS avg_visits
        FROM visits
        GROUP BY patient_id;
        """
        df = pd.read_sql(query, conn)
        print("Moyenne des visites par patient :")
        print(df)
        conn.close()

get_visits_for_patient('P001')
filter_visits_by_diagnosis_or_date(diagnosis='Depression')
filter_visits_by_diagnosis_or_date(start_date='2023-01-01', end_date='2023-03-01')
visits_per_month()
average_visits_per_patient()


