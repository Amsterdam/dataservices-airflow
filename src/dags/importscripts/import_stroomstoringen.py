import pandas as pd


def _pick_branch(file: str) -> str:
    """Select branch to be executed based on the amount of current power failures.

    Args:
        file: path to .geojson file containing info o power failures
    Returns:
        string containing name of desired branch
    """
    # Read .geojson file
    df = pd.read_json(file)

    # Check df on amount of power failures
    if len(df) >= 1:
        return "convert_datetime"
    else:
        return "drop_table"
