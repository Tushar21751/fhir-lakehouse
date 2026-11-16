import copy
import json
import os

DEFAULT_CONFIG_PATH = "/lakehouse/default/Files/config/pipeline_config.json"


def load_config(path=None, overrides=None):
    """Read the json config and apply notebook/pipeline overrides.

    overrides uses dotted keys, e.g. {"fhir.format": "xml"}. Empty strings and None
    are skipped so blank pipeline parameters fall back to the file.
    """
    path = path or os.environ.get("FHIR_PIPELINE_CONFIG") or DEFAULT_CONFIG_PATH
    with open(path, encoding="utf-8") as fh:
        cfg = json.load(fh)

    for dotted, value in (overrides or {}).items():
        if value is None or (isinstance(value, str) and value.strip() == ""):
            continue
        node = cfg
        keys = dotted.split(".")
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = value
    return cfg


def apply_lakehouse_root(cfg, root):
    """Point Files/Tables at an explicit root (abfss path or a local folder).

    Blank root keeps the defaults, which assume a default lakehouse is attached
    to the notebook (relative Files/ and Tables/ paths).
    """
    if not root:
        return cfg
    cfg = copy.deepcopy(cfg)
    root = root.rstrip("/")
    lh = cfg["lakehouse"]
    lh["files_root"] = f"{root}/Files"
    lh["tables_root"] = f"{root}/Tables"
    # a local folder can be written with plain python, abfss can't
    lh["files_mount"] = "" if root.startswith("abfss://") else f"{root}/Files"
    return cfg


def get_resource(cfg, name):
    for r in cfg["resources"]:
        if r["name"].lower() == str(name).strip().lower():
            if not r.get("enabled", True):
                raise ValueError(f"{r['name']} is disabled in the config")
            return r
    valid = ", ".join(r["name"] for r in cfg["resources"])
    raise ValueError(f"unknown resource '{name}', expected one of: {valid}")


def table_name(cfg, layer, resource_type):
    """bronze/silver table names, e.g. ('bronze', 'Patient') -> bronze_patient"""
    return f"{cfg['tables'][layer + '_prefix']}{resource_type.lower()}"


def gold_table(cfg, name):
    return f"{cfg['tables']['gold_prefix']}{name}"


def ctl_table(cfg, name):
    return f"{cfg['tables']['control_prefix']}{name}"
