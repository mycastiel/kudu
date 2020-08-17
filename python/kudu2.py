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

# Open a table
table = client.table('python')

# Create a new session so that we can apply write operations
session = client.new_session()
session1 = client.new_session()
session2 = client.new_session()
se3 = client.new_session()
s4 = client.new_session()

# Insert a row
# 往表里面插入一行数据
n = 3
while n < 100000:
    op1 = table.new_update({'key': n, 't1': 0, 't2':0})
    op2 = table.new_update({'key': n+80000, 't1': 0, 't2':0})
    op3 = table.new_update({'key': n+180000, 't1': 0, 't2':0})
    op4 = table.new_update({'key': n+280000, 't1': 0, 't2':0})
    op5 = table.new_update({'key': n+380000, 't1': 0, 't2':0})
    session.apply(op1)
    session1.apply(op2)
    session2.apply(op3)
    se3.apply(op4)
    s4.apply(op5)
    n = n + 1
# Upsert a row
# Updating a row

# Delete a row

# Flush write operations, if failures occur, capture print them.
try:
    session.flush()
    session1.flush()
    session2.flush()
    se3.flush()
    s4.flush()
except kudu.KuduBadStatus as e:
    print(session.get_pending_errors())
    print("bad")


