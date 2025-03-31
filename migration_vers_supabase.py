import psycopg2
import pandas as pd
import logging

# Fonction pour se connecter à PostgreSQL (base de données 'medical_records')
def create_postgresql_connection():
    try:
        connection = psycopg2.connect(
            dbname="medical_records",  # Nom de la base de données
            user="postgres",           # Utilisateur
            host="localhost",          # Hôte
            port="5432"                # Port
        )
        print("Connexion réussie à PostgreSQL!")
        return connection
    except Exception as e:
        logging.error(f"Erreur de connexion à PostgreSQL : {e}")
        return None

# Fonction pour se connecter à Supabase (qui utilise PostgreSQL)
def create_supabase_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",  
            user="postgres",    
            password="postgres",
            host="127.0.0.1",   
            port="54322"        
        )
        print("Connexion réussie à Supabase!")
        return conn
    except Exception as e:
        print(f"Erreur de connexion à Supabase : {e}")
        return None
    
#Le but est de créer une nv table dans PostgreSQL, la remplir puis la migrer vers Supabase :)
# Fonction pour créer la table `physician` dans PostgreSQL
def create_physician_table(conn):
    try:
        cur = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS physician (
            physician_id VARCHAR(10) PRIMARY KEY,
            physician_name VARCHAR(255),
            department VARCHAR(255)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Table 'physician' créée dans PostgreSQL.")
    except Exception as e:
        print(f"Erreur lors de la création de la table 'physician' dans PostgreSQL : {e}")

# Fonction pour insérer des données dans la table `physician` de PostgreSQL
def insert_physician_data(conn, fusion_data):
    try:
        cur = conn.cursor()
        for index, row in fusion_data.iterrows():
            insert_query = """
            INSERT INTO physician (physician_id, physician_name, department)
            VALUES (%s, %s, %s)
            ON CONFLICT (physician_id) DO NOTHING;
            """
            cur.execute(insert_query, (row['physician_id'], row['physician_name'], row['department']))
        conn.commit()
        print("Données insérées dans la table 'physician' dans PostgreSQL.")
    except Exception as e:
        print(f"Erreur lors de l'insertion des données : {e}")

# Fonction pour créer la table `physician` dans Supabase
def create_physician_table_supabase(conn):
    try:
        cur = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS physician (
            physician_id VARCHAR(10) PRIMARY KEY,
            physician_name VARCHAR(255),
            department VARCHAR(255)
        );
        """
        cur.execute(create_table_query)
        conn.commit()
        print("Table 'physician' créée dans Supabase.")
    except Exception as e:
        print(f"Erreur lors de la création de la table 'physician' dans Supabase : {e}")

# Fonction pour migrer les données de PostgreSQL vers Supabase
def migrate_to_supabase(postgres_conn, supabase_conn):
    try:
        # Charger les données de la table 'physician' depuis PostgreSQL
        query = "SELECT physician_id, physician_name, department FROM physician;"
        df = pd.read_sql(query, postgres_conn)
        
        # Créer la table 'physician' dans Supabase
        create_physician_table_supabase(supabase_conn)

        # Insérer les données dans la table 'physician' dans Supabase
        cur = supabase_conn.cursor()
        for index, row in df.iterrows():
            insert_query = """
            INSERT INTO physician (physician_id, physician_name, department)
            VALUES (%s, %s, %s)
            ON CONFLICT (physician_id) DO NOTHING;
            """
            cur.execute(insert_query, (row['physician_id'], row['physician_name'], row['department']))
        supabase_conn.commit()
        print("Données migrées vers Supabase.")
    except Exception as e:
        print(f"Erreur lors de la migration des données vers Supabase : {e}")

# Fonction principale pour effectuer les migrations
def migrate_physician_data():
    # Se connecter à PostgreSQL
    postgres_conn = create_postgresql_connection()

    if postgres_conn:
        
        fusion_data = pd.read_csv('/mnt/c/test-data/finall_data.csv') 
        create_physician_table(postgres_conn)
        insert_physician_data(postgres_conn, fusion_data)
        supabase_conn = create_supabase_connection()

        if supabase_conn:
            
            create_physician_table_supabase(supabase_conn)
            migrate_to_supabase(postgres_conn, supabase_conn)

        # Fermer les connexions
        postgres_conn.close()
        supabase_conn.close()

# Exécuter la migration
migrate_physician_data()
