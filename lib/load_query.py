def load_query(file_path: str) -> str:
    """
    Load query from file to string variable.

    Parameters
    ----------
    file_path: str
        Path to query file
    Returns
    -------
    query: str
    """
    with open(file_path, "r") as f:
        query = f.read()
    return query
