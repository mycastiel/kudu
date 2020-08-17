import kudu
from datetime import datetime
from kudu.client import Partitioning
from kudu.schema import Schema

type_mapper = {
    "int8": kudu.int8,
    "int16": kudu.int16,
    "int32": kudu.int32,
    "int64": kudu.int64,
    "float": kudu.float,
    "double": kudu.double,
    "decimal": kudu.decimal,
    "binary": kudu.binary,
    "string": kudu.string
}


class KuduClient:

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            return object.__new__(cls)
        return cls._instance

    def __init__(self):
        # Connect to Kudu master server
        self.client = kudu.onnect(host='master', port=7051)
        self.session = self.client.new_session()    # session没有关闭的方法

    @staticmethod
    def builder() -> kudu.schema:
        return kudu.schema_builder()

    @staticmethod
    def schema(builder: kudu.schema, columns: list) -> kudu.schema:
        """

        :param builder:
        :param columns: [
            {
                "name": "student_no",
                "type": "int32",
                "nullable": False,
                "primary_key": True
            }, {
                "name": "age",
                "type": "int8",
                "nullable": False,
                "primary_key": True
            }, {
                "name": "name",
                "type": "string",
                "nullable": True
            }, {
                "name": "gender",
                "type": "string",
                "nullable": True
            }
        ]
		:return:
        """
        primary_key = []

        for column in columns:
            if column.get("primary_key"):
                primary_key.append(column.get("name"))

            builder.add_column(
                name=column.get("name"),
                type_=type_mapper.get(column.get("type")),
                nullable=False if not column.get("nullable") else True,
                compression=column.get("compression"),
                encoding=column.get("encoding"),
                default=column.get("default"),
                block_size=column.get("block_size"),
                precision=column.get("precision"),
                scale=column.get("scale")
            )
        builder.set_primary_keys(primary_key)
        return builder.build()

    @staticmethod
    def partition(hash_columns: list, range_columns: list = None, bound: dict = None, bucket_num=3) -> Partitioning:
        # Define partitioning schema
        partition = Partitioning()
        for column in hash_columns:
            partition.add_hash_partitions(column_names=column, num_buckets=bucket_num)

        partition.set_range_partition_columns(range_columns)
        partition.add_range_partition(
            lower_bound=bound.get("lower_bound"),
            upper_bound=bound.get("upper_bound"),
            lower_bound_type=bound.get("lower_bound_type") or "inclusive",
            upper_bound_type=bound.get("upper_bound_type") or "exclusive"
        )

        return partition

    def add_column(self, table: kudu.Table, column: dict) -> None:
            """
			添加一列
            :param table:
            :param column:
            :return:
            """
            alter = self.new_table_alterer(table)
            alter.add_column(
                name=column.get("name"),
                type_=type_mapper.get(column.get("type")),
                nullable=False if not column.get("nullable") else True,
                compression=column.get("compression"),
                encoding=column.get("encoding"),
                default=column.get("default")
            )
            alter.alter()

    def add_range_partition(self, table: kudu.Table, bound: dict) -> None:
        """

        :param table:
        :param bound:{
        "lower_bound": {"create_time": datetime.now().strftime("%Y-%m-%d 00:00:00")},
        "upper_bound": {"create_time": datetime.now().strftime("%Y-%m-%d 23:59:59")}
        }
        :return:
        """
        alter = self.new_table_alterer(table)
        alter.add_range_partition(
            lower_bound=bound.get("lower_bound"),
            upper_bound=bound.get("upper_bound"),
            lower_bound_type=bound.get("lower_bound_type") or "inclusive",
            upper_bound_type=bound.get("upper_bound_type") or "exclusive"
        )
        alter.alter()

    def drop_range_partition(self, table: kudu.Table, bound: dict) -> None:
        alter = self.new_table_alterer(table)
        alter.drop_range_partition(
            lower_bound=bound.get("lower_bound"),
            upper_bound=bound.get("upper_bound"),
            lower_bound_type=bound.get("lower_bound_type") or "inclusive",
            upper_bound_type=bound.get("upper_bound_type") or "exclusive"
        )
        alter.alter()

    def show_tables(self) -> list:
        return self.list_tables()
    def create_table(self, table_name: str, schema: kudu.schema, partition: Partitioning, replica=3) -> None:
        # Create new table
        self.create_table(table_name, schema, partition, replica)

    def drop_table(self, table_name: str) -> None:
        self.delete_table(table_name)

    def table(self, table_name: str) -> kudu.Table:
        # Open a table
        return self.table(table_name)

    def insert(self, table: kudu.Table, rows: list) -> None:
            """

            :param table:
            :param rows: [{"student_no": 11, "age": 12, "name": "amy"}]
            :return:
            """
            for row in rows:
                op = table.new_insert(row)
                self.session.apply(op)
            try:
                self.session.flush()
            except kudu.KuduBadStatus:
                return self.session.get_pending_errors()

    @classmethod
    def __del(cls):
        cls._instance = None

    def __del__(self):
        self.client.close()
        self.__del()


if __name__ == '__main__':
    import time
    client = KuduClient()
    builder = client.builder()

    columns = [
        {
            "name": "student_no",
            "type": "int32",
            "nullable": False,
            "primary_key": True
        }, {
            "name": "age",
            "type": "int8",
            "nullable": False,
            "primary_key": True
        }, {
            "name": "create_time",
            "type": "string",
            "nullable": False,
            "primary_key": True
        }, {
            "name": "name",
            "type": "string",
            "nullable": True
        }, {
			"name": "gender",
            "type": "string",
            "nullable": True
        }
    ]

	# bound一定要用dict写，用list会莫名其妙把第一个定义的主键加入range partition
    bound = {
        "lower_bound": {"create_time": datetime.now().strftime("%Y-%m-%d 00:00:00")},
        "upper_bound": {"create_time": datetime.now().strftime("%Y-%m-%d 23:59:59")}
    }

    schema = client.schema(builder, columns)
    partition = client.partition(["student_no", "age"], ["create_time"], bound)
    print(partition.__dict__)

    #client.drop_table("python_kudu_test")
    client.create_table("python_kudu_test", schema, partition)

    table = client.table("python_kudu_test")
    # client.add_range_partition(table, bound)

	# 要确保插入的数据在range partition的范围内，否则无法插入也不会报错
    client.insert(table, [{"student_no": 11, "age": 12, "name": "amy", "create_time": ""}])
    print(client.show_tables())
