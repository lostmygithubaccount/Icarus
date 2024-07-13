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

    # preprocessing
    silver_buy_sell = bronze_buy_sell.pipe(preprocess)

    # extract nested records
    silver_buy_sell = silver_buy_sell.mutate(ibis._["buy_sell"].unnest()).unpack(
        "buy_sell"
    )

    # postprocessing
    silver_buy_sell = silver_buy_sell.pipe(postprocess)

    return silver_buy_sell


@dagster.asset()
def silver_social_media(bronze_social_media):
    """Silver ticker social media data."""

    # preprocessing
    silver_social_media = bronze_social_media.pipe(preprocess)

    # extract nested records
    silver_social_media = silver_social_media.unpack("social_media_post")

    # postprocessing
    silver_social_media = silver_social_media.pipe(postprocess)

    return silver_social_media
