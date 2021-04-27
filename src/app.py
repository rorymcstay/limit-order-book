#!/usr/bin/env python3
import argparse
from src.main import readCsvFile, read_query

parser = argparse.ArgumentParser()

parser.add_argument('--file', '-f')
parser.add_argument('--query', '-q')

if __name__ == '__main__':
    args = parser.parse_args()

    query = args.query
    file_name = args.file

    df = readCsvFile(file_name)
    query  = read_query(query, df)
    if not query:
        print('Query is not valid')
    elif query == '':
        print(df)
    else:
        print(df.query(query))

