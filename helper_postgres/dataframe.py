import psycopg2 as pg2
import pandas as pd

def fetch_dataframe(sql, credentials, notnull=True, save_as_csv=False):
    conn = pg2.connect(**credentials)
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    col_names = []
    for a in cur.description:
        col_names.append(a[0])
    df = pd.DataFrame(data, columns=col_names)
    if notnull:
        df = df.where(pd.notnull(df), None)
    if save_as_csv:
        df.to_csv(save_as_csv, index=False)
    return df