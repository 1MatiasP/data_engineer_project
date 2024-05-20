# Imagen base
FROM python:3.9

# Copiar el archivo de requisitos
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt
RUN airflow db init  # Inicializar la base de datos de Airflow

# Exponer el puerto web de Airflow
EXPOSE 8080

# Comando para iniciar Airflow
CMD ["airflow", "webserver", "--port", "8080"]
