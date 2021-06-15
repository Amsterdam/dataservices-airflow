#!/usr/bin/env python
import json
import pathlib

import yaml
from yamlip import fetch_interpolated_yaml

vars_path = pathlib.Path(__file__).resolve().parents[1] / "vars"
json_path = vars_path / "vars.json"
yaml_path = vars_path / "vars.yaml"

yaml_obj = yaml.safe_load(fetch_interpolated_yaml(yaml_path))
with open(json_path, "w") as json_file:
    json.dump(yaml_obj, json_file)
