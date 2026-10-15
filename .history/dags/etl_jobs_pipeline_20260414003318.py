@task()
def load_to_dwh(clean_path):
    df = pd.read_csv(clean_path)            # Lire un CSV
    
    hook = PostgresHook(postgres_conn_id="postgres_dwh")
    conn = hook.get_conn()                  # Récupérer une connexion
    cur = conn.cursor()                     # Créer un curseur
    
    for _, row in df.iterrows():            # Boucle Python sur les lignes
        cur.execute(
            "INSERT INTO fact_offres (...) VALUES (%s, %s, %s);",
            (row["titre"], row["entreprise"], row["ville"])
        )
    
    conn.commit()                           # Valider tous les inserts
    cur.close()
    conn.close()