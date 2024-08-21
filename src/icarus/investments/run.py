# imports
from icarus.config import BUY_SELL_TABLE, SOCIAL_MEDIA_TABLE
from icarus.catalog import Catalog
from icarus.investments.extract import (
    buy_sell as extract_buy_sell,
    social_media as extract_social_media,
)
from icarus.investments.transform import (
    buy_sell as transform_buy_sell,
    social_media as transform_social_media,
)


# functions
def main():
    # instantiate catalog
    catalog = Catalog()

    # extract
    extract_buy_sell_t = extract_buy_sell()
    extract_social_media_t = extract_social_media()

    # data validation
    assert (
        extract_buy_sell_t.count().to_pyarrow().as_py() > 0
    ), "No extracted buy/sell data"
    assert (
        extract_social_media_t.count().to_pyarrow().as_py() > 0
    ), "No extracted social media data"

    # transform
    transform_buy_sell_t = transform_buy_sell(extract_buy_sell_t)
    transform_social_media_t = transform_social_media(extract_social_media_t)

    # data validation
    assert (
        transform_buy_sell_t.count().to_pyarrow().as_py() > 0
    ), "No transformed buy/sell data"
    assert (
        transform_social_media_t.count().to_pyarrow().as_py() > 0
    ), "No transformed social media data"

    # load
    catalog.write_table(transform_buy_sell_t, BUY_SELL_TABLE)
    catalog.write_table(transform_social_media_t, SOCIAL_MEDIA_TABLE)
