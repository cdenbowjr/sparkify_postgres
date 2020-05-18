import psycopg2


def create_database(host, dbname, user, pword, dflt_dbase="postgres"):
    """
    :param name: Accepts database name in the form of string
    :return: Created database connection and cursor
    """
    # connecting the the default database
    try:
        conn = psycopg2.connect(host=host, database=dflt_dbase, user=user, password=pword)
        conn.set_session(autocommit=True)
        cur = conn.cursor()

    except psycopg2.Error as e:
        print("Error: Not connecting to the default database")
        print(e)

    # create new database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS " + dbname)
    cur.execute("CREATE DATABASE " + dbname + " WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database

    cur.close()
    conn.close()

    # connect to new database

    try:
        conn = psycopg2.connect(host=host, database=dbname, user=user, password=pword)
        conn.set_session(autocommit=True)
        cur = conn.cursor()

    except psycopg2.Error as e:
        print("Error: Not connecting to the new database")
        print(e)

    return conn, cur
