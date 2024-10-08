# imports
import os
import typer
import subprocess

from icarus.config import (
    DATA_DIR,
    RAW_DATA_DIR,
    PENGUINS_TABLE,
    BUY_SELL_TABLE,
    SOCIAL_MEDIA_TABLE,
)
from icarus.catalog import delta_table_filename
from icarus.penguins.run import main as penguins_run_main
from icarus.investments.run import main as investments_run_main
from icarus.synthetic_data.investments import (
    gen_buy_sell_batch,
    gen_social_media_batch,
)

# typer settings
TYPER_KWARGS = {
    "no_args_is_help": True,
    "add_completion": False,
    "context_settings": {"help_option_names": ["-h", "--help"]},
}

# typer apps
app = typer.Typer(help="Icarus: soaring beyond limits.", **TYPER_KWARGS)
run_app = typer.Typer(help="Run the ETL job.", **TYPER_KWARGS)
clean_app = typer.Typer(help="Clean the data lake.", **TYPER_KWARGS)

# add subcommands
app.add_typer(clean_app, name="clean")
app.add_typer(run_app, name="run")

# add subcommand aliases
app.add_typer(clean_app, name="c", hidden=True)
app.add_typer(run_app, name="r", hidden=True)


# helper functions
def check_raw_data_exists() -> bool:
    # check that the raw data exists
    if not os.path.exists(os.path.join(DATA_DIR, RAW_DATA_DIR)):
        typer.echo("run `icarus gen` first or use `--override`!")
        return False
    return True


def check_data_lake_exists() -> bool:
    # check that the data lake exists
    tables = [BUY_SELL_TABLE, SOCIAL_MEDIA_TABLE]
    tables = [delta_table_filename(table) for table in tables]
    for table in tables:
        if not os.path.exists(os.path.join(DATA_DIR, table)):
            typer.echo("run `icarus run` first or use `--override`!")
            return False
    return True


# commands
@app.command()
def gen():
    """Generate synthetic data."""
    try:
        typer.echo("generating data...")
        while True:
            gen_buy_sell_batch()
            gen_social_media_batch()

    except KeyboardInterrupt:
        typer.echo("stopping...")

    except Exception as e:
        typer.echo(f"error: {e}")


@run_app.command()
def penguins(
    override: bool = typer.Option(
        False, "--override", "-o", help="Override checks", show_default=True
    ),
):
    """Run ETL."""

    # ensure raw data exists
    if not override and not check_raw_data_exists():
        return

    # run the ETL job
    typer.echo("running ETL job...")
    penguins_run_main()


@run_app.command()
def investments(
    override: bool = typer.Option(
        False, "--override", "-o", help="Override checks", show_default=True
    ),
):
    """Run ETL."""

    # ensure raw data exists
    if not override and not check_raw_data_exists():
        return

    # run the ETL job
    typer.echo("running ETL job...")
    investments_run_main()


@app.command("app")
def main_app():
    """Open the app."""
    cmd = "shiny run -b apps/main.py"
    typer.echo(f"running: {cmd}...")
    subprocess.call(cmd, shell=True)


@clean_app.command("lake")
def clean_lake(
    override: bool = typer.Option(
        False, "--override", "-o", help="Override checks", show_default=True
    ),
):
    """Clean the data lake."""
    # ensure the data lake exists
    if not override and not check_data_lake_exists():
        return

    tables = [PENGUINS_TABLE, BUY_SELL_TABLE, SOCIAL_MEDIA_TABLE]
    tables = [delta_table_filename(table) for table in tables]

    for table in tables:
        cmd = f"rm -rf {os.path.join(DATA_DIR, table)}/"
        typer.echo(f"running: {cmd}...")
        subprocess.call(cmd, shell=True)


@clean_app.command("raw")
def clean_raw(
    override: bool = typer.Option(
        False, "--override", "-o", help="Override checks", show_default=True
    ),
    confirm: bool = typer.Option(
        True, "--confirm", "-c", help="Confirm deletion", show_default=True
    ),
):
    """Clean the raw data."""
    # ensure the data raw exists
    if not override and not check_raw_data_exists():
        return

    cmd = f"rm -rf {os.path.join(DATA_DIR, RAW_DATA_DIR)}/"
    typer.echo(f"running: {cmd}...")

    if confirm:
        typer.confirm("Are you sure you want to delete the raw data?", abort=True)

    subprocess.call(cmd, shell=True)


@clean_app.command("all")
def clean_all():
    """Clean all the data."""
    clean_lake()
    clean_raw()


if __name__ == "__main__":
    typer.run(app)
