from .commons import import_from_source


def pytest_sessionstart():
    """
    Adds src to PATH
    :return:
    """
    import_from_source.add_source_to_path()
    import_from_source.add_envs()


def pytest_sessionfinish():
    """
    Removes src from PATH
    :return:
    """
    import_from_source.remove_source_from_path()
    import_from_source.remove_envs()
