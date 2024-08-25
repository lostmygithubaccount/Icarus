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
def penguins(t: ibis.Table) -> ibis.Table:
    """Transform penguins data."""

    def transform(t):
        return t

    penguins = t.pipe(preprocess).pipe(transform).pipe(postprocess)
    return penguins
