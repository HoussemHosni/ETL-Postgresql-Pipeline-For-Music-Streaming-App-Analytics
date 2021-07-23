import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from db import get_db


def create_database(db, name):
    
    conn = psycopg2.connect(host=db["host"],
                            database=db["database"],
                            user=db["user"],
                            password=db["password"])
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    conn.close()
    conn = psycopg2.connect(host=db["host"],
                            database=name,
                            user=db["user"],
                            password=db["password"])
    cur = conn.cursor()
    return conn, cur
    
def drop_tables(conn, cur):
    
    for drop_query in drop_table_queries:
        cur.execute(drop_query)
        conn.commit()
        
def create_tables(conn, cur):
    
    for create_query in create_table_queries:
        cur.execute(create_query)
        conn.commit()
        
    
def main():
    db = get_db('database.ini', 'postgresql')
    conn, cur = create_database(db, 'sparkifydb')
    drop_tables(conn, cur)
    create_tables(conn, cur)
    conn.close()
    
if __name__ == '__main__':
    main()

