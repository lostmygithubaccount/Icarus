# Icarus Investments

***work in progress***

## setup

Clone the repo:

```bash
gh repo clone lostmygithubaccount/icarus-investments
```

Change into it:
    
```bash
cd icarus-investments
```

Setup a Python virtual environment (an exercise left to the reader).

You should probably install `just` and use the commands in the [`justfile`](justfile) like `just setup`.

Alternatively, you can:

```bash
pip install -r dev-requirements.txt
pip install -e .
```

Then the `icarus` CLI is available. Use it to generate data and run stuff.
