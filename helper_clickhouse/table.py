from clickhouse_driver import Client

def create_table(table_name, field_types, client : Client):
    sql = """ 
CREATE TABLE IF NOT EXISTS {table_name} ( 
    {field} 
) 
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 8192
         """
    fields = [f"\n{col} Nullable({dt})" for col,dt in field_types.items()]
        
    query = sql.format(table_name=table_name,field=",".join(fields))
    client.execute(query)