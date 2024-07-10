import os
import time
import uuid
import ibis
import ibis.expr.datatypes as dt

from faker import Faker
from datetime import datetime

from icarus_investments.dag.config import DATA_DIR, RAW_DATA_DIR

fake = Faker()

os.makedirs(os.path.join(DATA_DIR, RAW_DATA_DIR), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, RAW_DATA_DIR, "buy_sell"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, RAW_DATA_DIR, "social_media"), exist_ok=True)

seed_data = {
    "tickers": [
        {
            "ticker": "AA",
            "name": "Apache Arrow",
            "shares_total": 10_000_000,
            "base_price": 100.0,
            "trend": "down",
        },
        {
            "ticker": "IBIS",
            "name": "Ibis project",
            "shares_total": 100_000_000,
            "base_price": 150.0,
            "trend": "up",
        },
        {
            "ticker": "II",
            "name": "Icarus Investments",
            "shares_total": 500_000,
            "base_price": 3_000.0,
            "trend": "up",
        },
        {
            "ticker": "SUBS",
            "name": "Substrait",
            "shares_total": 1_000_000,
            "base_price": 1_000.0,
            "trend": "down",
        },
        {
            "ticker": "VODA",
            "name": "Voltron Data",
            "shares_total": 1_000_000,
            "base_price": 2_500.0,
            "trend": "flat",
        },
    ]
}
seed_table = (
    ibis.memtable(seed_data)["tickers"]
    .lift()
    .select(
        "ticker",
        "name",
        "base_price",
        "shares_total",
        "trend",
    )
)

assert (
    seed_table.count().to_pyarrow().as_py()
    == seed_table["ticker"].nunique().to_pyarrow().as_py()
), "Tickers must be unique"


# udfs
@ibis.udf.scalar.python
def _buy_sell(
    base_price: float, shares_total: int, trend: str, batch_size: int = 2**7
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
) -> dt.Struct(
    {
        "timestamp": datetime,
        "content": str,
        "location": list[str],
    }
):
    now = datetime.now()

    res = {
        "timestamp": now,
        "content": f"on {ticker}: {fake.sentence()}",
        "location": fake.location_on_land(),
    }

    return res


# functions
def gen_buy_sell_batch(seed_table=seed_table):
    buy_sell_data = (
        seed_table.mutate(
            buy_sell=_buy_sell(
                base_price=seed_table["base_price"],
                shares_total=seed_table["shares_total"],
                trend=seed_table["trend"],
            )
        )
        .mutate(ibis._["buy_sell"].unnest())
        .unpack("buy_sell")
    ).drop("name", "base_price", "shares_total", "trend")

    buy_sell_data.to_parquet(
        os.path.join(
            DATA_DIR, RAW_DATA_DIR, "buy_sell", f"{time.time()}+{uuid.uuid4()}.parquet"
        ),
    )


def gen_social_media_batch(seed_table=seed_table):
    social_media_data = seed_table.select("ticker").mutate(
        social_media_post=_social_media_post(seed_table["ticker"])
    )

    social_media_data.to_parquet(
        os.path.join(
            DATA_DIR,
            RAW_DATA_DIR,
            "social_media",
            f"{time.time()}+{uuid.uuid4()}.parquet",
        ),
    )
