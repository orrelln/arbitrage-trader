import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'mytestdb'
TABLES = {}


ids = []
with open('input/exchanges.txt', 'r') as f:
    for line in f:
        ids.append(line.strip())
print(ids)

for idx in ids:
    TABLES[idx] = (
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

cnx = mysql.connector.connect(user='b1kxsssu6ej4', password='7sxgckl5ndg1wj7la8esqvxw', host='mytestdb.cg7oi5h8vvxm.us-east-2.rds.amazonaws.com',
                              port='3306', database='mytestdb')
cursor = cnx.cursor()

for name, ddl in TABLES.items():
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

