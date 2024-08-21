# imports
import os
import ibis

from datetime import datetime
from icarus.config import (
    DATA_DIR,
    RAW_DATA_DIR,
    BUY_SELL_TABLE,
    SOCIAL_MEDIA_TABLE,
)

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
def buy_sell() -> ibis.Table:
    """Extract ticker buy/sell data"""

    # read in raw data
    data_glob = os.path.join(DATA_DIR, RAW_DATA_DIR, BUY_SELL_TABLE, "*.parquet")
    buy_sell = ibis.read_parquet(data_glob)

    # add extracted_at column
    buy_sell = buy_sell.pipe(add_extracted_at)

    return buy_sell


def social_media() -> ibis.Table:
    """Extract ticker social media data"""

    # read in raw data
    data_glob = os.path.join(DATA_DIR, RAW_DATA_DIR, SOCIAL_MEDIA_TABLE, "*.parquet")
    social_media = ibis.read_parquet(data_glob)

    # add extracted_at column
    social_media = social_media.pipe(add_extracted_at)

    return social_media
