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

def query_sipv_data_and_insert_to_new_instance_db(instance_id, database_id, new_instance_id, new_database_id):
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
            # print("StableIdPropertyValueId: {}, ResourceType: {}, PropertyValue: {}, StableId {}".format(*row))

            def insert_StableIdPropertyValue(transaction):
                row_ct = transaction.execute_update(
                    "INSERT INTO StableIdPropertyValue (StableIdPropertyValueId,ResourceType,PropertyValue,StableId) "
                    " VALUES ('{}', '{}', '{}','{}')".format(*row)
                )

                print("{} record(s) inserted.".format(row_ct))

            new_database.run_in_transaction(insert_StableIdPropertyValue)

def query_sir_data_and_insert_to_new_instance_db(instance_id, database_id, new_instance_id, new_database_id):
    """Queries sample data from the database using SQL."""

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    q_spanner_client = spanner.Client()
    q_instance = q_spanner_client.instance(new_instance_id)
    q_database = q_instance.database(new_database_id)
    q_list = []
    with q_database.snapshot() as q_snapshot:
        q_results = q_snapshot.execute_sql(
            "select distinct StableIdPropertyValueId from StableIdPropertyValue limit 100"
        )
        for q_row in q_results:
            print("StableIdPropertyValueId: {}".format(*q_row))
            q_list.append(q_row)
    # new_q_list = ','.join('?' for i in range(len(q_list)))
    new_q_list = sum(q_list, [])
    # print(new_q_list)
    
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            # "select StableIdResourcesId,ResourceType,ResourceId,StableIdPropertyValueId from StableIdResources where StableIdPropertyValueId in (?)",(q_list,)
            "select StableIdResourcesId,ResourceType,ResourceId,StableIdPropertyValueId from StableIdResources where StableIdPropertyValueId in ({})".format(str(new_q_list)[1:-1])
            # "select StableIdResourcesId,ResourceType,ResourceId,StableIdPropertyValueId from StableIdResources where StableIdPropertyValueId in ({})".format(new_q_list)
        )

        # select StableIdResourcesId,ResourceType,ResourceId,StableIdPropertyValueId from StableIdResources --where StableIdPropertyValueId in (select StableIdPropertyValueId from StableIdPropertyValue limit 10)
        # delete from StableIdResources where StableIdPropertyValueId in (select StableIdPropertyValueId from StableIdPropertyValue limit 10)
        new_spanner_client = spanner.Client()
        new_instance = new_spanner_client.instance(new_instance_id)
        new_database = new_instance.database(new_database_id)
        for row in results:
            print("StableIdPropertyValueId: {}, ResourceType: {}, ResourceId: {}, StableIdPropertyValueId {}".format(*row))

            def insert_StableIdPropertyValue(transaction):
                row_ct = transaction.execute_update(
                    "INSERT INTO StableIdResources (StableIdResourcesId,ResourceType,ResourceId,StableIdPropertyValueId) "
                    " VALUES ('{}', '{}', '{}','{}')".format(*row)
                )

                print("{} record(s) inserted.".format(row_ct))

            new_database.run_in_transaction(insert_StableIdPropertyValue)

