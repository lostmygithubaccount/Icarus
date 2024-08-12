import dagster


# gold data assets
@dagster.asset()
def gold_buy_sell(silver_buy_sell):
    """Gold ticker buy/sell data."""
    return silver_buy_sell


@dagster.asset()
def gold_social_media(silver_social_media):
    """Gold ticker social media data."""
    return silver_social_media
