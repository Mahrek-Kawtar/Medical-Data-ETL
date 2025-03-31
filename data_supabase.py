import psycopg2
import pandas as pd
import logging

# Fonction pour établir la connexion avec Supabase
def create_supabase_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="127.0.0.1",  
            port="54322"  
        )
        logging.info("Connexion réussie")
        return conn
    except Exception as e:
        logging.error(f"Erreur de connexion à Supabase : {e}")
        return None

# Fonction pour créer les tables dans Supabase
def create_supabase_tables():
    conn = create_supabase_connection()
    cur = conn.cursor()
    
    # Création des tables dans Supabase
    cur.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            patient_id VARCHAR(30) PRIMARY KEY,
            age INTEGER,
            gender VARCHAR(50),
            patient_notes TEXT
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS visits (
            visit_id VARCHAR(30) PRIMARY KEY,
            patient_id VARCHAR(30) REFERENCES patients(patient_id),
            visit_date DATE,
            diagnosis TEXT,
            visit_notes TEXT,
            visit_count INTEGER,
            age_group VARCHAR(50)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS medications (
            medication_id VARCHAR(30) PRIMARY KEY,
            medication_medication VARCHAR(255)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS prescriptions (
            prescription_id SERIAL PRIMARY KEY,
            visit_id VARCHAR(30) REFERENCES visits(visit_id),
            medication_id VARCHAR(30) REFERENCES medications(medication_id),
            dosage VARCHAR(50),
            start_date DATE,
            end_date DATE,
            medication_notes TEXT,
            medication_duration INTEGER,
            CONSTRAINT visit_medication_unique UNIQUE (visit_id, medication_id)
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()

# Fonction pour insérer les données dans Supabase
def insert_supabase_data(fusion_data):
    try:
        logging.info("Début de l'insertion des données fusionnées")

        conn = create_supabase_connection()
        cur = conn.cursor()

        for index, row in fusion_data.iterrows():
            logging.info(f"Traitement de la ligne {index + 1}: patient_id = {row['patient_id']}")

            # Log des valeurs avant insertion
            logging.info(f"Valeurs pour patient {row['patient_id']}: "
                         f"age = {row['age']}, gender = {row['gender']}, patient_notes = {row['patient_notes']}")

            # Gestion des NaN dans 'age', on remplace NaN par une valeur par défaut (ici 0)
            if pd.isna(row['age']):
                logging.warning(f"L'âge du patient {row['patient_id']} est manquant. Remplacement par 0.")
                row['age'] = 0  

            # Vérification si l'âge est dans la plage d'un integer valide
            if row['age'] < -2147483648 or row['age'] > 2147483647:  # Limites d'un integer en PostgreSQL
                logging.error(f"Valeur 'age' hors de portée pour le patient {row['patient_id']}: {row['age']}")
                continue  # Passe à la ligne suivante

            # Validation et insertion dans la table patients
            patient_values = (row['patient_id'], row['age'], row['gender'], row['patient_notes'])
            try:
                logging.info(f"Insertion dans la table patients: {patient_values}")
                cur.execute("""
                    INSERT INTO patients (patient_id, age, gender, patient_notes)
                    VALUES (%s, %s, %s, %s) ON CONFLICT (patient_id) DO NOTHING
                """, patient_values)
                logging.info(f"Patient {row['patient_id']} inséré avec succès")
            except Exception as e:
                logging.error(f"Erreur lors de l'insertion dans la table patients pour {row['patient_id']} : {e}")

            # Validation et insertion dans la table visits
            visit_values = (row['visit_id'], row['patient_id'], row['visit_date'], row['diagnosis'], 
                            row['visit_notes'], row['visit_count'], row['age_group'])
            try:
                logging.info(f"Insertion dans la table visits: {visit_values}")
                cur.execute("""
                    INSERT INTO visits (visit_id, patient_id, visit_date, diagnosis, visit_notes, visit_count, age_group)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (visit_id) DO NOTHING
                """, visit_values)
                logging.info(f"Visite {row['visit_id']} insérée avec succès")
            except Exception as e:
                logging.error(f"Erreur lors de l'insertion dans la table visits pour {row['visit_id']} : {e}")

            # Validation et insertion dans la table medications
            medication_values = (row['medication_id'], row['medication_medication'])
            try:
                logging.info(f"Insertion dans la table medications: {medication_values}")
                cur.execute("""
                    INSERT INTO medications (medication_id, medication_medication)
                    VALUES (%s, %s) ON CONFLICT (medication_id) DO NOTHING
                """, medication_values)
                logging.info(f"Médicament {row['medication_id']} inséré avec succès")
            except Exception as e:
                logging.error(f"Erreur lors de l'insertion dans la table medications pour {row['medication_id']} : {e}")

            # Validation et insertion dans la table prescriptions
            prescription_values = (row['visit_id'], row['medication_id'], row['dosage'], row['start_date'], 
                                   row['end_date'], row['medication_notes'], row['medication_duration'])
            try:
                logging.info(f"Insertion dans la table prescriptions: {prescription_values}")
                cur.execute("""
                    INSERT INTO prescriptions (visit_id, medication_id, dosage, start_date, end_date, medication_notes, medication_duration)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (visit_id, medication_id) DO NOTHING
                """, prescription_values)
                logging.info(f"Prescription pour {row['visit_id']} insérée avec succès")
            except Exception as e:
                logging.error(f"Erreur lors de l'insertion dans la table prescriptions pour {row['visit_id']} et {row['medication_id']} : {e}")

        conn.commit()
        cur.close()
        conn.close()
        logging.info("Toutes les données ont été insérées avec succès.")

    except Exception as e:
        logging.error(f"Erreur lors de l'insertion générale des données : {e}")
        raise


# Charger les données depuis le fichier CSV de la data fusion 
fusion_data = pd.read_csv('/mnt/c/test-data/finall_data.csv')

# Exécution des fonctions :)
create_supabase_tables()
insert_supabase_data(fusion_data)
