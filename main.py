import psycopg2
import matplotlib.pyplot as plt


username = 'AndyLan'
password = '123'
database = 'lab2'
host = 'localhost'
port = '5432'

query_1 = '''
CREATE VIEW PoemPeriod AS
SELECT period_name, COUNT(poem_name)
FROM poems
INNER JOIN periods ON poems.genre_id = periods.period_id
GROUP BY period_name
'''
query_2 = '''
CREATE VIEW PoemAuthor AS
SELECT author_name, COUNT(poem_name)
FROM poems
INNER JOIN authors ON poems.author_id = authors.author_id
GROUP BY author_name
'''

query_3 = '''
CREATE VIEW PoemGenre AS
SELECT genre_name, COUNT(poem_name)
FROM poems
INNER JOIN genres ON poems.genre_id = genres.genre_id
GROUP BY genre_name
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
with conn:
    cur = conn.cursor()

    cur.execute('DROP VIEW IF EXISTS PoemPeriod')

    cur.execute(query_1)
    cur.execute('SELECT * FROM PoemPeriod')
    periods = []
    p_count = []

    for row in cur:
        periods.append(row[0])
        p_count.append(row[1])


    cur.execute('DROP VIEW IF EXISTS PoemPeriod')

    cur.execute(query_2)
    cur.execute('SELECT * FROM PoemAuthor')
    authors = []
    a_count = []

    for row in cur:
        fullname = row[0].split()
        authors.append(fullname[0][0] + '. ' + fullname[1])
        a_count.append(row[1])
    

    cur.execute('DROP VIEW IF EXISTS PoemPeriod')

    cur.execute(query_3)
    cur.execute('SELECT * FROM PoemGenre')
    genres = []
    g_count = []

    for row in cur:
        genres.append(row[0])
        g_count.append(row[1])

    fig, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)
    
    # bar
    bar_ax.set_title('Кількість віршів, написаних у конкретний період')
    bar_ax.set_xlabel('Період')
    bar_ax.set_ylabel('Кількість віршів')
    bar = bar_ax.bar(periods, p_count)
    bar_ax.set_xticks(range(len(periods)))
    bar_ax.set_xticklabels(periods, rotation=30)

    # pie
    pie_ax.pie(a_count, labels=authors, autopct='%1.1f%%')
    pie_ax.set_title('Кількість віршів окремого автора')

    # graph
    graph_ax.plot(genres, g_count, marker='o')
    graph_ax.set_xlabel('Жанр')
    graph_ax.set_ylabel('Кількість віршів')
    graph_ax.set_title('Графік залежності кількості віршів від жанру')
    for gnr, count in zip(genres, g_count):
        graph_ax.annotate(count, xy=(gnr, count), xytext=(7, 2), textcoords='offset points')


plt.get_current_fig_manager().resize(1400, 600)
plt.show()
