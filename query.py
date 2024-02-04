from google.cloud import spanner
from google.type import expr_pb2
# from google.cloud import spanner
# from google.cloud.spanner_admin_instance_v1.types import spanner_instance_admin
# from google.cloud.spanner_v1 import param_types
# from google.cloud.spanner_v1 import DirectedReadOptions

def query_data(instance_id, database_id, new_instance_id, new_database_id):
    """Queries sample data from the database using SQL."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "select StableIdPropertyValueId,ResourceType,PropertyValue,StableId from StableIdPropertyValue limit 100"
        )

        new_spanner_client = spanner.Client()
        new_instance = new_spanner_client.instance(new_instance_id)
        new_database = new_instance.database(new_database_id)
        for row in results:
            print("StableIdPropertyValueId: {}, ResourceType: {}, PropertyValue: {}, StableId {}".format(*row))

def insert_data_with_dml(instance_id, database_id):
    """Inserts sample data into the given database using a DML statement.
    """
    # [START spanner_dml_standard_insert]
    # instance_id = "your-spanner-instance"
    # database_id = "your-spanner-db-id"

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    def insert_singers(transaction):
        row_ct = transaction.execute_update(
            "INSERT INTO Singers (SingerId, FirstName, LastName) "
            " VALUES (10, 'Virginia', 'Watson')"
        )

        print("{} record(s) inserted.".format(row_ct))

    database.run_in_transaction(insert_singers)
    # [END spanner_dml_standard_insert]
