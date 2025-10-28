import pyodbc

conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:ds-projekt.database.windows.net,1433;"
    "Database=db_projekt;"
    "Uid=ds_projekt_CGK;"
    "Pwd=7Df6M4Ym$H@F@5Hs;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

try:
    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        cur.execute("SELECT SUSER_SNAME(), DB_NAME(), @@VERSION;")
        print(cur.fetchone())
    print("✅ Anslutning OK")
except Exception as e:
    print("❌ Fel vid anslutning:", e)