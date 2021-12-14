import csv
import psycopg2


username = 'AndyLan'
password = '123'
database = 'lab2'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'lab3/kagglepoems.csv'

query = '''
INSERT INTO poems (poem_name, body, author_id, period_id, genre_id) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (poem_name) DO NOTHING
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()

    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf)

        for row in reader:          
            cur.execute('SELECT DISTINCT author_id FROM authors WHERE author_name = (%s)', (row['author'],))
            author_id = cur.fetchone()
            cur.execute('SELECT DISTINCT period_id FROM periods WHERE period_name = (%s)', (row['age'],))
            period_id = cur.fetchone()
            cur.execute('SELECT DISTINCT genre_id FROM genres WHERE genre_name = (%s)', (row['type'].split()[0],))
            genre_id = cur.fetchone()
            
            print(f'a: {author_id}, p: {period_id}, g: {genre_id}')
            if author_id is not None and period_id is not None and genre_id is not None:
                values = (row['poem name'], row['content'][:239], author_id[0], period_id[0], genre_id[0])
                print(values)
                cur.execute(query, values)
                
    conn.commit()
