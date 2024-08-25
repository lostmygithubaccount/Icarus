# imports
import ibis

from datetime import datetime

# set extracted_at timestamp
# note we don't use ibis.now() to ensure it's the same...
# ...for all tables/rows on a given run
extracted_at = datetime.utcnow().isoformat()


# functions
def add_extracted_at(t: ibis.Table) -> ibis.Table:
    """Add extracted_at column to table"""

    # add extracted_at column and relocate it to the first position
    t = t.mutate(extracted_at=ibis.literal(extracted_at)).relocate("extracted_at")

    return t


# data assets
def penguins() -> ibis.Table:
    """Extract penguins data"""

    # read in raw data
    penguins = ibis.examples.penguins.fetch()

    # add extracted_at column
    penguins = penguins.pipe(add_extracted_at)

    return penguins
