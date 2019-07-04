from flask_mysqldb import MySQL


def create_MySQL_connection():
    return MySQL()


mysql = create_MySQL_connection()
