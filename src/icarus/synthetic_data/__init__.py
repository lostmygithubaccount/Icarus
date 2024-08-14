import os
import time
import uuid
import ibis
import ibis.expr.datatypes as dt

from faker import Faker
from datetime import datetime

from icarus.config import (
    DATA_DIR,
    RAW_DATA_DIR,
    RAW_BUY_SELL_TABLE,
    RAW_SOCIAL_MEDIA_TABLE,
)
from icarus.investments.dag.assets.seed import data

# setup faker
fake = Faker()

# create directories
os.makedirs(os.path.join(DATA_DIR, RAW_DATA_DIR), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, RAW_DATA_DIR, "buy_sell"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, RAW_DATA_DIR, "social_media"), exist_ok=True)

# read seed data into table
seed_table = (
    ibis.memtable(data)
    .unpack("tickers")
    .select(
        "ticker",
        ibis._["type"].name("sector"),  # work around column name conflict
        "name",
        "base_price",
        "shares_total",
        "trend",
    )
)

# enforce seed data constraints
assert (
    seed_table.count().to_pyarrow().as_py()
    == seed_table["ticker"].nunique().to_pyarrow().as_py()
), "Tickers must be unique"


# udfs
@ibis.udf.scalar.python
def _buy_sell(
    base_price: float,
    shares_total: int,
    sector: str,
    trend: str,
    batch_size: int = 2**7,
) -> dt.Array(
    dt.Struct(
        {
            "timestamp": datetime,
            "type": str,
            "shares": float,
            "price": float,
            "location": list[str],
        }
    )
):
    """
    Generate records of buy/sell data
    """

    now = datetime.now()

    if trend == "up":
        res = batch_size * [
            {
                "timestamp": now,
                "type": "buy",
                "shares": 0.001 * shares_total,
                "price": base_price + (0.001 * base_price),
                "location": fake.location_on_land(),
            },
        ]

    else:
        res = batch_size * [
            {
                "timestamp": now,
                "type": "sell",
                "shares": 0.001 * shares_total,
                "price": base_price - (0.001 * base_price),
                "location": fake.location_on_land(),
            },
        ]

    return res


@ibis.udf.scalar.python
def _social_media_post(
    ticker: str,
    sector: str,
) -> dt.Struct(
    {
        "timestamp": datetime,
        "content": str,
        "location": list[str],
    }
):
    """
    Generate records of social media post data
    """

    now = datetime.now()

    res = {
        "timestamp": now,
        "content": f"on {ticker} in {sector}: {fake.sentence()}",
        "location": fake.location_on_land(),
    }

    return res


# functions
def _write_batch(batch, table_name, data_dir=DATA_DIR, raw_data_dir=RAW_DATA_DIR):
    batch.to_parquet(
        os.path.join(
            data_dir,
            raw_data_dir,
            table_name,
            f"{time.time()}+{uuid.uuid4()}.parquet",
        ),
    )


def gen_buy_sell_batch(seed_table=seed_table):
    buy_sell_data = (
        seed_table.mutate(
            buy_sell=_buy_sell(
                base_price=seed_table["base_price"],
                sector=seed_table["sector"],
                shares_total=seed_table["shares_total"],
                trend=seed_table["trend"],
            )
        )
    ).drop("sector", "name", "base_price", "shares_total", "trend")

    _write_batch(buy_sell_data, RAW_BUY_SELL_TABLE)


def gen_social_media_batch(seed_table=seed_table):
    social_media_data = (
        seed_table.select("ticker", "sector")
        .mutate(
            social_media_post=_social_media_post(
                ticker=seed_table["ticker"], sector=seed_table["sector"]
            )
        )
        .drop("sector")
    )

    _write_batch(social_media_data, RAW_SOCIAL_MEDIA_TABLE)
