import os
import ibis
import dagster

from datetime import datetime
from icarus.config import (
    DATA_DIR,
    RAW_DATA_DIR,
    RAW_BUY_SELL_TABLE,
    RAW_SOCIAL_MEDIA_TABLE,
)

# set ingested_at timestamp
ingested_at = datetime.utcnow().isoformat()


# bronze functions
def add_ingested_at(t):
    """Add ingested_at column to table"""

    # add ingested_at column and relocate it to the first position
    t = t.mutate(ingested_at=ibis.literal(ingested_at)).relocate("ingested_at")

    return t


# bronze data assets
@dagster.asset
def bronze_buy_sell():
    """Bronze ticker buy/sell data"""

    # read in raw data
    data_glob = os.path.join(DATA_DIR, RAW_DATA_DIR, RAW_BUY_SELL_TABLE, "*.parquet")
    bronze_buy_sell = ibis.read_parquet(data_glob)

    # add ingested_at column
    bronze_buy_sell = bronze_buy_sell.pipe(add_ingested_at)

    return bronze_buy_sell


@dagster.asset
def bronze_social_media():
    """Bronze ticker social media data"""

    # read in raw data
    data_glob = os.path.join(
        DATA_DIR, RAW_DATA_DIR, RAW_SOCIAL_MEDIA_TABLE, "*.parquet"
    )
    bronze_social_media = ibis.read_parquet(data_glob)

    # add ingested_at column
    bronze_social_media = bronze_social_media.pipe(add_ingested_at)

    return bronze_social_media
