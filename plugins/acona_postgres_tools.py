from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
import psycopg2.extras

def acona_truncate_table(table):
    sql = "TRUNCATE TABLE {}".format(table)

    warehouse = PostgresHook(postgres_conn_id='acona_data_warehouse')
    warehouse_uri = warehouse.get_uri()
    engine = create_engine(warehouse_uri)
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def acona_data_write(table, data):
    from sqlalchemy import create_engine
    import psycopg2.extras

    data_columns_list = list(data)
    # create (col1,col2,...)
    data_columns = ",".join(data_columns_list)

    # create VALUES('%s', '%s",...) one '%s' per column
    data_values = "VALUES({})".format(",".join(["%s" for _ in data_columns_list]))

    #create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = "INSERT INTO {} ({}) {}".format(table,data_columns,data_values)

    warehouse = PostgresHook(postgres_conn_id='acona_data_warehouse')
    warehouse_uri = warehouse.get_uri()
    engine = create_engine(warehouse_uri)
    conn = engine.raw_connection()
    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, insert_stmt, data.values)
    conn.commit()
    cur.close()

def acona_fetch_data(sql):
    try:
        warehouse = PostgresHook(postgres_conn_id='acona_data_warehouse')
        warehouse_uri = warehouse.get_uri()
        engine = create_engine(warehouse_uri)
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        return result
    except (Exception) as error:
         print("Error while fetching data from PostgreSQL", error)
    finally:
      # closing database connection.
      if conn:
         cur.close()
         conn.close()

def acona_fetch_one(sql):
    try:
        warehouse = PostgresHook(postgres_conn_id='acona_data_warehouse')
        warehouse_uri = warehouse.get_uri()
        engine = create_engine(warehouse_uri)
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchone()
        return result
    except (Exception) as error:
         print("Error while fetching data from PostgreSQL", error)
    finally:
      # closing database connection.
      if conn:
         cur.close()
         conn.close()