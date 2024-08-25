# imports
from icarus.config import PENGUINS_TABLE
from icarus.catalog import Catalog
from icarus.penguins.extract import (
    penguins as extract_penguins,
)
from icarus.penguins.transform import penguins as transform_penguins


# functions
def main():
    # instantiate catalog
    catalog = Catalog()

    # extract
    extract_penguins_t = extract_penguins()

    # transform
    transform_penguins_t = transform_penguins(extract_penguins_t)

    # load
    catalog.write_table(transform_penguins_t, PENGUINS_TABLE)
