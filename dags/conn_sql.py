from mysql.connector import Error
import mysql.connector as msql
from sqlalchemy import create_engine

# conexões com o bando de dados


def open_connection():
    conn = create_engine("mysql://root:mysql_321654@192.168.1.8:3306", echo=False)
    return conn
# criação do banco de dados db


def create_db():
    try:
        cnx = msql.connect(host='192.168.1.8', user='root', password='mysql_321654', port='3306')
        if cnx.is_connected():
             cursor = cnx.cursor()
             cursor.execute("CREATE DATABASE db")

    except Error as e:
        print(e)
# criação do banco de dados analytic


def create_analytic():
    try:
        cnx = msql.connect(host='192.168.1.8', user='root', password='mysql_321654', )
        if cnx.is_connected():
             cursor = cnx.cursor()
             cursor.execute("CREATE DATABASE analytic")

    except Error as e:
        print(e)

#create_db()
