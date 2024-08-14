import os
import ibis

from icarus.config import DATA_DIR


# define data catalog
class Catalog:
    def __init__(self, data_dir=DATA_DIR):
        self.data_dir = data_dir

    def list_groups(self):
        return [
            d
            for d in os.listdir(self.data_dir)
            if not (d.startswith("_") or d.startswith("."))
        ]

    def list_tables(self, group):
        return [d.split(".")[0] for d in os.listdir(os.path.join(self.data_dir, group))]

    def table(self, table_name, group_name=None):
        if group_name is None:
            group_name = table_name.split("_")[0].upper()
        return ibis.read_delta(f"{DATA_DIR}/{group_name}/{table_name}.delta")
