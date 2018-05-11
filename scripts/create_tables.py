import mysql.connector
from mysql.connector import errorcode


def create_tables(ids, connector):
    tables = {}

    for idx in ids:
        tables[idx] = (
            "CREATE TABLE `" + idx + "` ("
            "  `symbol` varchar(10) NOT NULL,"
            "  `datetime` datetime NOT NULL,"
            "  `high` double,"
            "  `low` double,"
            "  `bid` double,"
            "  `ask` double,"
            "  `baseVolume` double,"
            "  `quoteVolume` double,"
            "  PRIMARY KEY (`symbol`, `datetime`)"
            ") ENGINE=InnoDB")

    cnx = mysql.connector.connect(**connector)
    cursor = cnx.cursor()

    for name, ddl in tables.items():
        try:
            print("Creating table {}: ".format(name), end='')
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cursor.close()
    cnx.close()


def create_pairs_tables(connector):
    SQL = (
        "CREATE TABLE `PAIRS` ("
        "   `label` varchar(5) NOT NULL,"
        "   `body` text NOT NULL,"
        "   PRIMARY KEY (`label`)"
        ")  ENGINE=InnoDB")

    cnx = mysql.connector.connect(**connector)
    cursor = cnx.cursor()
    cursor.execute(SQL)

    cursor.close()
    cnx.close()

