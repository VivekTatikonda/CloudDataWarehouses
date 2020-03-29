import configparser
import psycopg2
from sql_queries import analytics_queries


def analytics(cur, conn):
    for query in analytics_queries:
        print(query)
        cur.execute(query)
        row=cur.fetchone()
        for a in row:
            print(a)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    analytics(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()