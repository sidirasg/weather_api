import logging
try:
    from .database import *
except:
    from experiment.weather.Database.database import *


log = logging.getLogger(__name__)
isConnectorOpened = False


def open_connector():
    """
    Sets up database connection from config file.
    Input:
    config_file_name: File containing host, user,
                      password, database, port, which are the
                      credentials for the PostgreSQL database
    """
    params = config()
    Database.initialise(**params)


def close_connector():
    """
    Close database connection.
    """
    Database.close_all_connections()


def create_to_db(asset, colums):
    """
    Creates Table from schema.ini using data types.
    Input:
    asset: database table name
    columns: database table columns
    """
    subquery = ''
    for key, val in colums.items():  # for name, age in dictionary.iteritems():  (for Python 2.x)
        subquery = subquery + str(key) + ' ' + str(val) + ' NOT NULL ,'
    subquery = subquery[:-1]
    subquery = subquery + ');'

    query = 'CREATE TABLE ' + asset + '(id SERIAL PRIMARY KEY, ' + subquery
    return query


def connector(*args):
    """
    --------------------------------------------------------
    The main function that handles all the possible scenarios for
    interaction with the persistence postgres Database
    --------------------------------------------------------
    ARGUMENTS scenarios:
        1. asset name , 1 : Create table in the Database if it is not created
        2. asset name , 0 : Drop table from the Database
        3. asset name , 1 , [...] : Write input values in asset name Table of Database , The values can be in a list or in a sqquance
        4. asset name , 0 , [...] : Read specific columns that are determined in input array values from the input asset
                                    Table of Database.If the input array from *args is empty then all columns of the
                                    Table are returned.
        5.asset name

    --------------------------------------------------------
    """
    if len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], int):
        # Create
        if args[1] == 1:
            columns = configuration(args[0])
            if not check_existing(args[0]):
                query = create_to_db(args[0], columns)
                val = execute_query(query, 0)
            else:
                val = 'ERROR'
                return val
        # Destroy
        elif args[1] == 0:
            query = remove_from_db(args[0])
            val = execute_query(query, 0)

    elif len(args) == 3 and isinstance(args[0], str) and isinstance(args[1], int) and isinstance(args[2], list):
        # Read
        if args[1] == 0:
            values = []
            for a in args[2]:
                values.append(a)
            query = read_from_db(args[0], values)
            val = execute_query(query, 1)
        # Write
        elif args[1] == 1:
            columns = configuration(args[0])
            values = []
            for a in args[2]:
                values.append(a)
            query = write_to_db(args[0], columns, values)
            val = execute_query(query, 0)
    if str(val) == 'None':
        return 'OK'
    else:
        return val


def check_existing(asset):
    """
    Checking the existing of a Table(asset) in the Database.
    Input:
        asset: database table name
    """
    with CursorFromConnectionPool() as cursor:
        cursor.execute("select * from information_schema.tables where table_name=%s", (str(asset).lower(),))
        return bool(cursor.rowcount)


def read_from_db(asset, values):
    """
    Read values from Checking the existing of a Table(asset) in the Database.
    Input:
        asset: database table name
    Output:
        values: Selected desired columns from input Table Schema
    """
    query = 'SELECT '
    if not values:
        query = query + '*'
    else:
        for i in values:
            query = query + str(i) + ' ,'
        # delete the last comma
        query = query[:-1]
    query = query + ' FROM ' + asset + ';'
    # print(query)
    return query


def write_to_db(asset, columns, values):
    """
    Write values from Checking the existing of a Table(asset) in the Database.
    Input:
        asset: database table name
    Output:
        values: Selected desired columns from input Table Schema
    """
    query = 'INSERT INTO ' + asset + ' ('
    for i in columns:
        query = query + str(i) + ' ,'
        # delete the last comma
    query = query[:-1]
    query = query + ') '
    query = query + ' VALUES ('
    for i in values:
        if type(i)==list:
                for j in i:
                    if isinstance(j, str):
                        query = query + "'" + str(j) + "'" + ' ,'
                    else:
                        query = query + str(j) + ' ,'

        else:
            if isinstance(i, str):
                query = query + "'" + str(i) + "'" + ' ,'
            else:
                query = query + str(i) + ' ,'
    query = query[:-1]
    query = query + ');'
    print(query)
    return query


def remove_from_db(asset):
    """
    Remove a Table(asset) from the Database.
    Input:
        asset: database table name
    """
    query = 'DROP TABLE ' + asset + ';'
    return query


def configuration(section, filename='settings/schema.ini'):

    """
    Configuration and validation of database schema.
    Input:
    schema.ini: File containing section that represents the table name,
                                and properties the column name with there own data types
    """
    # create a parserα
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    columns = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            columns[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return columns


def execute_query(query, action):
    """
    Execution Natively query string statement
    Input:
    query: The statement written in SQL
    action: pointer for returning data pulled from database or not
    """
    try:
        with CursorFromConnectionPool() as cursor:
            if action == 1:
                cursor.execute(query)
                return cursor.fetchall()
            else:
                return cursor.execute(query)
    except:
        return 'ERROR'


if __name__ == '__main__':
    try:
        open_connector()  # OPEN SESSION FROM POOL
        val = connector('ElectorGenerator', 1)  # CREATE TABLE
        print(val)
        val = connector('ElectorGenerator', 1, [1, 'sfcd', 'sfdce', 'sfcew', 'sdcwe', 10])  # INSERT VALUES
        print(val)
        val = connector('ElectorGenerator', 0, [])  # READ EVERYTHING
        print(val)
        val = connector('ElectorGenerator', 0, ['PrimarySource', 'efficiency'])  # READ 2 COLUMNS
        print(val)
        val = connector('ElectorGenerator', 0)  # DROP TABLE
        print(val)
        close_connector()  # CLOSE SESSION
    except:
        pass
