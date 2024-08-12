# justfile

# load environment variables
set dotenv-load

# variables
module := "icarus.investments.dag"
package := "icarus-cds"

# aliases
alias fmt:=format
alias install:=setup

# list justfile recipes
default:
    just --list

# build
build:
    just clean-dist
    @python -m build

# setup
setup:
    @pip install -r dev-requirements.txt

# uninstall
uninstall:
    @pip uninstall -y {{package}}

# format
format:
    @ruff format .

# publish-test
release-test:
    just build
    @twine upload --repository testpypi dist/* -u __token__ -p ${PYPI_TEST_TOKEN}

# publish
release:
    just build
    @twine upload dist/* -u __token__ -p ${PYPI_TOKEN}

# clean dist
clean-dist:
    @rm -rf dist

# preview
preview:
    @quarto preview website

# app
app:
    @shiny run apps/main.py -b 