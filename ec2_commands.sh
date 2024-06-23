sudo apt update
sudo apt install python3-pip
sudo apt install python3.12-venv
python3 -m venv venv
source venv/bin/activate
sudo venv/bin/pip install pandas s3fs apache-airflow
airflow standalone