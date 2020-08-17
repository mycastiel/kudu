import kudu
from kudu.client import Partitioning
from datetime import datetime

# Connect to Kudu master server
# 连接 kudu master 服务
client = kudu.connect(host='master', port=7051)

# Define a schema for a new table
# 为表定义一个模式
builder = kudu.schema_builder()
builder.add_column('key').type(kudu.int64).nullable(False).primary_key()
builder.add_column('t1', type_=kudu.int64)
builder.add_column('t2', type_=kudu.int64)
schema = builder.build()

# Define partitioning schema
partitioning = Partitioning().add_hash_partitions(column_names=['key'], num_buckets=3)

# Create new table
client.create_table('python3', schema, partitioning)

# Open a table
table = client.table('python3')
i = 0
# Create a new session so that we can apply write operations
while i<100:
    session = client.new_session()
    n = 0
    print(i)
    while n < 100000:
        op1 = table.new_insert({'key': n+i*100000, 't1': 0, 't2':0})
        session.apply(op1)
        n = n + 1
    try:
        session.flush()
    except kudu.KuduBadStatus as e:
        print(session.get_pending_errors())
        print("bad")
    i=i+1

