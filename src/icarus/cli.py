import os
import typer
import subprocess

from icarus.synthetic_data import (
    gen_buy_sell_batch,
    gen_social_media_batch,
)
from icarus.config import (
    DAG_MODULE,
    DATA_DIR,
    RAW_DATA_DIR,
    BRONZE,
    SILVER,
    GOLD,
)

TYPER_KWARGS = {
    "no_args_is_help": True,
    "add_completion": False,
    "context_settings": {"help_option_names": ["-h", "--help"]},
}
app = typer.Typer(help="Icarus", **TYPER_KWARGS)
clean_app = typer.Typer(help="Clean the data lake.", **TYPER_KWARGS)

## add subcommands
app.add_typer(clean_app, name="clean")

## add subcommand aliases
app.add_typer(clean_app, name="c", hidden=True)


# helper functions
def check_raw_data_exists() -> bool:
    # check that the raw data exists
    if not os.path.exists(os.path.join(DATA_DIR, RAW_DATA_DIR)):
        typer.echo("run `icarus gen` first or use `--override`!")
        return False
    return True


def check_data_lake_exists() -> bool:
    # check that the data lake exists
    for metal in [BRONZE, SILVER, GOLD]:
        if not os.path.exists(os.path.join(DATA_DIR, metal)):
            typer.echo("run `icarus run` first or use `--override`!")
            return False
    return True


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


@app.command()
def gui():
    """Start the dagster webserver/GUI."""
    cmd = f"dagster dev -m {DAG_MODULE}"
    subprocess.call(cmd, shell=True)


@app.command()
def run(
    job_name: str = typer.Option(
        "all_assets",
        "--job-name",
        "-j",
        help="Name of the job to run",
        show_default=True,
    ),
    override: bool = typer.Option(
        False, "--override", "-o", help="Override checks", show_default=True
    ),
):
    """Run ETL."""

    # ensure raw data exists
    if not override and not check_raw_data_exists():
        return

    # materialize all assets
    cmd = f"dagster job execute -j {job_name} -m {DAG_MODULE}"
    subprocess.call(cmd, shell=True)


@app.command("app")
def main_app():
    """Open the app."""
    cmd = "shiny run -b apps/main.py"
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

    medals = [BRONZE, SILVER, GOLD]

    for metal in medals:
        cmd = f"rm -rf {os.path.join(DATA_DIR, metal)}/"
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

    if confirm:
        typer.confirm("Are you sure you want to delete the raw data?", abort=True)

    cmd = f"rm -rf {os.path.join(DATA_DIR, RAW_DATA_DIR)}/"
    typer.echo(f"running: {cmd}...")
    subprocess.call(cmd, shell=True)


@clean_app.command("all")
def clean_all():
    """Clean all the data."""
    clean_lake()
    clean_raw()


if __name__ == "__main__":
    typer.run(app)
