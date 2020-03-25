import json
import dynamic_yaml
from dynamic_yaml.yaml_wrappers import YamlDict, YamlList
import pathlib


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, YamlDict):
            return {k: o.__getitem__(k) for k, v in o._collection.items()}
        if isinstance(o, YamlList):
            return o._collection
        return super().default(self, o)


def _unwrap(obj):
    if isinstance(obj, YamlDict):
        return {k: _unwrap(v) for k, v in obj._collection.items()}
    elif isinstance(obj, YamlList):
        return [_unwrap(item) for item in obj._collection]
    return obj


vars_path = pathlib.Path(__file__).resolve().parents[1] / "vars"
json_path = vars_path / "vars.json"
yaml_path = vars_path / "vars.yaml"
with open(yaml_path) as yaml_file:
    yaml_obj = dynamic_yaml.load(yaml_file)
with open(json_path, "w") as json_file:
    # json.dump(_unwrap(yaml_obj), json_file)  # , cls=CustomJsonEncoder)
    json.dump(yaml_obj, json_file, cls=CustomJsonEncoder)
