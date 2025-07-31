import argparse

from google.cloud import bigtable
from google.cloud.bigtable.row_set import RowSet


def main(project_id="qwiklabs-asl-01-e660751acd56", instance_id="phonelogs", table_id="phone_user_activity"):
    # Create a Cloud Bigtable client.
    client = bigtable.Client(project=project_id)

    # Connect to an existing Cloud Bigtable instance.
    instance = client.instance(instance_id)

    # Open an existing table.
    table = instance.table(table_id)

    start_key = "010ceb22-8933-4668-974b-0956fceb8644#UserActivityRecord#1746141133"
    end_key = "010ceb22-8933-4668-974b-0956fceb8644#UserActivityRecord#1746141134"
    
    column_family_id = "raw"
    column_id = "Raw".encode("utf-8")
    
    row_set = RowSet()
    row_set.add_row_range_from_keys(start_key, end_key)
    
    rows = table.read_rows(row_set=row_set)
    for row in rows:
        print(row.cells[column_family_id][column_id][0].value.decode("utf-8"))


if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
#     )
#     parser.add_argument("project_id", help="Your Cloud Platform project ID.")
#     parser.add_argument(
#         "instance_id", help="ID of the Cloud Bigtable instance to connect to."
#     )
#     parser.add_argument(
#         "--table", help="Existing table used in the quickstart.", default="my-table"
#     )

#     args = parser.parse_args()
    main()