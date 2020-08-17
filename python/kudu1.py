import kudu
from kudu.client import Partitioning
from datetime import datetime

# Connect to Kudu master server
client = kudu.connect(host='master', port=7051)

# Define a schema for a new table
builder = kudu.schema_builder()
builder.add_column('key').type(kudu.int64).nullable(False).primary_key()
builder.add_column('t1', type_=kudu.int64)
builder.add_column('t2', type_=kudu.int64)
schema = builder.build()

# Define partitioning schema
partitioning = Partitioning().add_hash_partitions(column_names=['key'], num_buckets=3)

# Create new table
#client.delete_table('python1')
client.create_table('python', schema, partitioning)

# Open a table
table = client.table('python')

# Create a new session so that we can apply write operations
session = client.new_session()
s1 = client.new_session()
s2 = client.new_session()
s3 = client.new_session()
s4 = client.new_session() 

# Insert a row
n = 0
while n < 100000:
    op = table.new_insert({'key': n, 't1': n, 't2':n})
    op1 = table.new_insert({'key': n+100000, 't1': n, 't2':n})
    op2 = table.new_insert({'key': n+200000, 't1': n, 't2':n})
    op3 = table.new_insert({'key': n+300000, 't1': n, 't2':n})
    op4 = table.new_insert({'key': n+400000, 't1': n, 't2':n})
    session.apply(op)
    s1.apply(op1)
    s2.apply(op2)
    s3.apply(op3)
    s4.apply(op4)
    n = n + 1
# Upsert a row
op = table.new_update({'key': 2, 't1': 20160101, 't2':1})
session.apply(op)

# Updating a row

# Delete a row
op = table.new_delete({'key': 1})
session.apply(op)

# Flush write operations, if failures occur, capture print them.
try:
    s1.flush()
    session.flush()
    s2.flush()
    s3.flush()
    s4.flush()
except kudu.KuduBadStatus as e:
    print(session.get_pending_errors())

# Create a scanner and add a predicate
scanner = table.scanner()
scanner.add_predicate(table['t1'] == 20160101)

# Open Scanner and read all tuples
# Note: This doesn't scale for large scans
result = scanner.open().read_all_tuples()
