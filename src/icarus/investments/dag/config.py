# configuration file for the data DAG
DAG_MODULE = "icarus.investments.dag"

CLOUD = False
BUCKET = "icarus"

DATA_DIR = "datalake"
RAW_DATA_DIR = "_raw"
RAW_BUY_SELL_TABLE = "buy_sell"
RAW_SOCIAL_MEDIA_TABLE = "social_media"

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
