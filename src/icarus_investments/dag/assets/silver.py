import ibis
import dagster
import ibis.selectors as s


# silver functions
def preprocess(t):
    """Common preprocessing steps from silver assets"""

    # ensure unique records
    t = t.distinct(on=~s.c("ingested_at"), keep="first").order_by("ingested_at")

    return t


def postprocess(t):
    """Common postprocessing steps for silver assets"""

    # ensure consistent column casing
    t = t.rename("snake_case")

    return t


# silver data assets
@dagster.asset()
def silver_buy_sell(bronze_buy_sell):
    """Silver ticker buy/sell data."""

    def transform(t):
        t = t.mutate(t["buy_sell"].unnest()).unpack("buy_sell")
        return t

    silver_buy_sell = bronze_buy_sell.pipe(preprocess).pipe(transform).pipe(postprocess)
    return silver_buy_sell


@dagster.asset()
def silver_social_media(bronze_social_media):
    """Silver ticker social media data."""

    def transform(t):
        t = t.unpack("social_media_post")
        return t

    silver_social_media = (
        bronze_social_media.pipe(preprocess).pipe(transform).pipe(postprocess)
    )
    return silver_social_media
