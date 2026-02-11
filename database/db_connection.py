from sqlalchemy import create_engine
import yaml
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(BASE_DIR, "config", "settings.yaml")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

DB_NAME = config["database"]["name"]

engine = create_engine(f"sqlite:///{DB_NAME}", echo=False)
