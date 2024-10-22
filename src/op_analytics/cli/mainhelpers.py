import os


def find_apps(path: str):
    """Finds all python modules in the path.

    This is used to auto-discover CLI subcommands based on our directory structure convention.
    """
    apps = []

    for basename in os.listdir(path):
        name = os.path.join(path, basename)
        if os.path.isdir(name) and os.path.isfile(os.path.join(name, "__init__.py")):
            apps.append(basename)

        if all(
            [
                os.path.isfile(name),
                basename.endswith(".py"),
                not basename.endswith("__init__.py"),
            ]
        ):
            apps.append(basename)

        app_names = []
        for basename in apps:
            app_names.append(basename.removesuffix(".py"))

    return sorted(app_names)
