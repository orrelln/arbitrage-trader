from scripts.create_tables import create_tables
from scripts.create_tables import create_pairs_tables
from scripts.file_reader import read_sql_information


def main():
    connector = read_sql_information()
    ids = []
    with open('input/exchanges.txt', 'r') as f:
        for line in f:
            ids.append(line.strip())
    create_tables(ids, connector)
    create_pairs_tables(connector)


if __name__ == '__main__':
    main()