# imports
from dagster import Definitions

from icarus_investments.dag.jobs import jobs
from icarus_investments.dag.assets import assets
from icarus_investments.dag.resources import resources

# definitions
defs = Definitions(
    assets=assets,
    resources=resources,
    jobs=jobs,
)
