import os


def find_apps(path: str):
    """Finds all python modules in the path.

    This is used to auto-discover CLI subcommands based on our directory structure convention.
    """
    apps = []

    for childname in os.listdir(path):
        name = os.path.join(path, childname)
        if os.path.isdir(name) and os.path.isfile(os.path.join(name, "__init__.py")):
            apps.append(childname)

        if all(
            [
                os.path.isfile(name),
                childname.endswith(".py"),
                not childname.endswith("__init__.py"),
            ]
        ):
            apps.append(childname)

    app_names = []
    for childname in apps:
        app_names.append(childname.removesuffix(".py"))

    return sorted(app_names)
