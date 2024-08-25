# imports
import ibis
import ibis.selectors as s


# functions
def preprocess(t: ibis.Table) -> ibis.Table:
    """Common preprocessing steps"""

    # ensure unique records
    t = t.distinct(on=~s.c("extracted_at"), keep="first").order_by("extracted_at")

    return t


def postprocess(t: ibis.Table) -> ibis.Table:
    """Common postprocessing steps"""

    # ensure consistent column casing
    t = t.rename("snake_case")

    return t


# data assets
def buy_sell(t: ibis.Table) -> ibis.Table:
    """Transform ticker buy/sell data."""

    def transform(t):
        t = t.mutate(t["buy_sell"].unnest()).unpack("buy_sell")
        return t

    buy_sell = t.pipe(preprocess).pipe(transform).pipe(postprocess)
    return buy_sell


def social_media(t: ibis.Table) -> ibis.Table:
    """Transform ticker social media data."""

    def transform(t):
        t = t.unpack("social_media_post")
        return t

    social_media = t.pipe(preprocess).pipe(transform).pipe(postprocess)
    return social_media
