import ibis
import plotly.express as px

from shiny import reactive, render
from shinyswatch import theme
from shinywidgets import render_plotly
from shiny.express import input, ui

from datetime import datetime, timedelta

from icarus.investments.dag.resources import Catalog

catalog = Catalog()

buy_sell_t = catalog.table("gold_buy_sell")
social_media_t = catalog.table("gold_social_media")

# dark themes
# px.defaults.template = "plotly_dark"
ui.page_opts(theme=theme.sketchy)

# page options
ui.page_opts(
    title="Icarus Investments",
    fillable=False,
    full_width=True,
)

# add page title and sidebar
with ui.sidebar(open="desktop"):
    ui.input_date_range(
        "date_range",
        "Date range",
        start=(datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )
    ui.input_action_button("last_7d", "Last 7 days")
    ui.input_action_button("last_14d", "Last 14 days")
    ui.input_action_button("last_28d", "Last 28 days")
    ui.input_action_button("last_91d", "Last 91 days")
    ui.input_action_button("last_182d", "Last 182 days")
    ui.input_action_button("last_365d", "Last 365 days")
    ui.input_action_button("last_all", "All available data")

with ui.nav_panel("Whatever"):
    with ui.value_box(full_screen=True):
        "Total rows"

        @render.express
        def total_rows():
            f"{buy_sell_t.count().to_pyarrow().as_py():,}"

    with ui.card(full_screen=True):
        "Some data"

        @render.data_frame
        def downloads_roll():
            t = buy_sell_data()

            return render.DataGrid(t.limit(10000).to_polars())


# reactive calculations and effects
@reactive.calc
def date_range():
    start_date, end_date = input.date_range()

    return start_date, end_date


@reactive.calc
def buy_sell_data(buy_sell_t=buy_sell_t):
    start_date, end_date = input.date_range()

    t = buy_sell_t
    # t = t.filter(ownloads_t["date"] >= start_date, downloads_t["date"] <= end_date)

    return t


@reactive.calc
def _buy_sell_data(buy_sell_t=buy_sell_t):
    t = buy_sell_t

    return t


@reactive.effect
@reactive.event(input.last_7d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_14d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_28d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_91d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=91)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_182d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=182)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_365d)
def _():
    ui.update_date_range(
        "date_range",
        start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        end=datetime.now().strftime("%Y-%m-%d"),
    )


@reactive.effect
@reactive.event(input.last_all)
def _():
    # TODO: pretty hacky
    min_all_tables = [
        (col, t[col].cast("timestamp").min().to_pyarrow().as_py())
        for t in [_buy_sell_data()]
        for col in t.columns
        if ("timestamp" in str(t[col].type()) or "date" in str(t[col].type()))
    ]
    min_all_tables = min([x[1] for x in min_all_tables]) - timedelta(days=1)
    max_now = datetime.now() + timedelta(days=1)

    ui.update_date_range(
        "date_range",
        start=(min_all_tables).strftime("%Y-%m-%d"),
        end=max_now.strftime("%Y-%m-%d"),
    )
