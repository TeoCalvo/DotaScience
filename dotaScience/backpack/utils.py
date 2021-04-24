import toml
import os

TOML_PATH = os.path.join( os.path.expanduser("~"), "dota.toml" )

def import_toml(path = TOML_PATH):
    return toml.load(open(path, "r"))