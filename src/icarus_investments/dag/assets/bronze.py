import ibis
import dagster

from datetime import datetime
from icarus_investments.dag.config import DATA_DIR, RAW_DATA_DIR

ingested_at = datetime.utcnow().isoformat()


def add_ingested_at(t):
    return t.mutate(ingested_at=ibis.literal(ingested_at)).relocate("ingested_at")


@dagster.asset
def bronze_buy_sell():
    """Bronze ticker buy/sell data"""
    bronze_buy_sell = ibis.read_parquet(f"{DATA_DIR}/{RAW_DATA_DIR}/buy_sell/*.parquet")
    bronze_buy_sell = bronze_buy_sell.pipe(add_ingested_at)

    return bronze_buy_sell


@dagster.asset
def bronze_social_media():
    """Bronze ticker social media data"""
    bronze_social_media = ibis.read_parquet(
        f"{DATA_DIR}/{RAW_DATA_DIR}/social_media/*.parquet"
    )
    bronze_social_media = bronze_social_media.pipe(add_ingested_at)

    return bronze_social_media
