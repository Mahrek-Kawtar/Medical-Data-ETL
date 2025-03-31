import psycopg2
import pandas as pd
import logging
import pdb

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def create_connection():
    try:
        connection = psycopg2.connect(
            dbname="medical_records",  # Nom de la base de données que j'ai créé 
            user="postgres",           
            host="localhost",          
            port="5432"                
        )
        print("Connexion réussie !")
        return connection
    except OperationalError as e:
        logging.error(f"Erreur de connexion : {e}")
        return None

def create_tables():
    conn = create_connection()
    cur = conn.cursor()

    # Table patients
    cur.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            patient_id VARCHAR(30) PRIMARY KEY,
            age INTEGER,
            gender VARCHAR(50),
            patient_notes TEXT
        )
    """)

    # Table visits
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

    # Table medications
    cur.execute("""
        CREATE TABLE IF NOT EXISTS medications (
            medication_id VARCHAR(30) PRIMARY KEY,
            medication_medication VARCHAR(255)
        )
    """)

    # Table prescriptions
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

def insert_fusion_data(fusion_data):
    try:
        logging.info("Début de l'insertion des données fusionnées")

        conn = create_connection()
        cur = conn.cursor()

        for index, row in fusion_data.iterrows():
            logging.info(f"Traitement de la ligne {index + 1}: patient_id = {row['patient_id']}")

            # Utilisation conditionnelle de pdb pour le débogage (à activer en mode debug uniquement)
            if logging.getLogger().level == logging.DEBUG:
                pdb.set_trace()

            # on remplace NaN par une valeur par défaut (ici 0) pour que cela passe dans la database :)
            if pd.isna(row['age']):
                logging.warning(f"L'âge du patient {row['patient_id']} est manquant. Remplacement par 0.")
                row['age'] = 0  

            # Validation et insertion dans la table patients
            patient_values = (row['patient_id'], row['age'], row['gender'], row['patient_notes'])
            try:
                cur.execute("""
                    INSERT INTO patients (patient_id, age, gender, patient_notes)
                    VALUES (%s, %s, %s, %s) ON CONFLICT (patient_id) DO NOTHING
                """, patient_values)
                logging.info(f"Patient {row['patient_id']} inséré avec succès")
            except Exception as e:
                logging.error(f"Erreur lors de l'insertion dans la table patients pour {row['patient_id']} : {e}")

            # Validation et insertion dans la table visits
            visit_values = (row['visit_id'], row['patient_id'], row['visit_date'], row['diagnosis'], row['visit_notes'], row['visit_count'], row['age_group'])
            try:
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
                cur.execute("""
                    INSERT INTO medications (medication_id, medication_medication)
                    VALUES (%s, %s) ON CONFLICT (medication_id) DO NOTHING
                """, medication_values)
                logging.info(f"Médicament {row['medication_id']} inséré avec succès")
            except Exception as e:
                logging.error(f"Erreur lors de l'insertion dans la table medications pour {row['medication_id']} : {e}")

            # Validation et insertion dans la table prescriptions
            prescription_values = (row['visit_id'], row['medication_id'], row['dosage'], row['start_date'], row['end_date'], row['medication_notes'], row['medication_duration'])
            try:
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


if __name__ == "__main__":
   
    create_tables()
    fusion_data = pd.read_csv('/mnt/c/test-data/finall_data.csv')
    insert_fusion_data(fusion_data)
