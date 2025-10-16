import yaml

from os import path


class Config:
    def __init__(self, config_file: str) -> None:
        if path.exists(config_file):
            self.load_config(config_file)
        
    def load_config(self, config_file_path: str) -> None:
        """Loads configuration from a YAML file."""
        with open(config_file_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
