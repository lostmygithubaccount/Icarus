# imports
from dagster import Definitions

from icarus.investments.dag.jobs import jobs
from icarus.investments.dag.assets import assets
from icarus.investments.dag.resources import resources

# definitions
defs = Definitions(
    assets=assets,
    resources=resources,
    jobs=jobs,
)
