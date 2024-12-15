import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        dbname="postgres",
        user="metabase_admin",
        password="wFRH@Uuerfhq@I23R3EJU",
        host="147.78.130.225",
        port="5432",  # Default PostgreSQL port
    )

def fetch_query(query, params=None):
    """Execute a SELECT query and return results as a list of dictionaries."""
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result
    finally:
        conn.close()