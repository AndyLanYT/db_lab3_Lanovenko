import csv
import psycopg2

username = 'AndyLan'
password = '123'
database = 'lab2'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'lab3/kagglepoems.csv'

query_0 = '''
CREATE TABLE poems_new
(
    poem_id integer,
    author character(50),
    content text,
    poem_name character varying(255),
    age character varying(40),
    type character varying(1000),
    CONSTRAINT pk_products_new PRIMARY KEY (poem_id)
)
'''

query_1 = '''
DELETE FROM poems_new
'''

query_2 = '''
INSERT INTO poems_new (poem_id, author, content, poem_name, age, type) VALUES (%s, %s, %s, %s, %s, %s)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS poems_new')
    cur.execute(query_0)
    cur.execute(query_1)

    with open(INPUT_CSV_FILE, 'r') as file:
        reader = csv.DictReader(file)
        for idx, row in enumerate(reader):
            values = (idx, row['author'], row['content'], row['poem name'], row['age'], row['type']) 
            cur.execute(query_2, values)

    conn.commit()
