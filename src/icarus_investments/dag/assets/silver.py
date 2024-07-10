import dagster
import ibis.selectors as s


def common(t):
    t = t.rename("snake_case")
    t = t.order_by("ingested_at")
    t = t.distinct(on=~s.c("ingested_at"))
    return t


@dagster.asset()
def silver_buy_sell(bronze_buy_sell):
    """Silver ticker buy/sell data."""
    silver_buy_sell = bronze_buy_sell.pipe(common)
    return silver_buy_sell


@dagster.asset()
def silver_social_media(bronze_social_media):
    """Silver ticker social media data."""
    silver_social_media = bronze_social_media.pipe(common)
    return silver_social_media
