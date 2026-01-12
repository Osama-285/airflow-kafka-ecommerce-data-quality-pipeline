FROM apache/airflow:3.0.3

# Switch to airflow user before installing Python packages
USER airflow

# Copy requirements first (better cache layer)
COPY requirements.txt /requirements.txt

# Install additional Airflow providers and dependencies
RUN pip install --no-cache-dir -r /requirements.txt
