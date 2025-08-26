FROM apache/airflow:2.8.2-python3.11

# Switch to root to install system packages
USER root

# Install prerequisites and Microsoft ODBC Driver 17 for SQL Server
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        apt-transport-https \
        lsb-release \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python packages from requirements
RUN pip install --no-cache-dir \
        pyodbc \
        python-dotenv \
        pandas \
        sqlalchemy \
        openpyxl \
        xlsxwriter \
    pyyaml \
    python-docx \
    pymssql \
    glob2