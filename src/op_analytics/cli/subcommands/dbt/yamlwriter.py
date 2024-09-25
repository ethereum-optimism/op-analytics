import yaml
from yaml.representer import SafeRepresenter


def custom_str_representer(dumper, data):
    """Pretty long strings.

    This custom representer forces using the | block style when dumping long strings to yaml.
    """
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return SafeRepresenter.represent_str(dumper, data)


yaml.add_representer(str, custom_str_representer)


def write_sources_yaml(fobj, sources: dict):
    yaml.dump(sources, fobj, sort_keys=False)
