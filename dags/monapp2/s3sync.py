from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_local import S3ToLocalOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

# Configuration de la DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
}

with DAG(
        's3_to_filesystem',
        default_args=default_args,
        description='Détecter un fichier sur S3 et le transférer vers un serveur local',
        schedule_interval=None,  # Déclenchement manuel ou par événement
        start_date=datetime(2024, 1, 1),
        catchup=False,
) as dag:

    # Étape 1 : Détecter un fichier dans un répertoire S3
    detect_file = S3KeySensor(
        task_id='detect_file',
        bucket_name='nom-du-bucket-s3',
        bucket_key='chemin/vers/repertoire/*.csv',  # Expression Globale pour correspondre au fichier
        aws_conn_id='aws_default',  # Connexion configurée dans l'UI Airflow
        timeout=600,  # Timeout de la tâche (en secondes)
        poke_interval=30,  # Intervalle entre chaque tentative
    )

    # Étape 2 : Télécharger le fichier depuis S3 vers le serveur Airflow
    download_file = S3ToLocalOperator(
        task_id='download_file',
        s3_bucket='nom-du-bucket-s3',
        s3_key='chemin/vers/repertoire/nom_du_fichier.csv',  # Peut être dynamique si utilisé avec XCom
        local_file='/tmp/nom_du_fichier.csv',
        aws_conn_id='aws_default',
    )

    # Étape 3 : Copier le fichier vers un autre serveur via SSH
    transfer_file = SSHOperator(
        task_id='transfer_file',
        ssh_conn_id='ssh_server_conn',  # Connexion SSH configurée dans Airflow
        command='scp /tmp/nom_du_fichier.csv user@target_server:/chemin/cible/',
    )

    # Dépendances des tâches
    detect_file >> download_file >> transfer_file
