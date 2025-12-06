import os
import yaml


class BaseConfig:
    """
    Load and merge configuration from specified YAML files

    Usage:
        # Load all YAML files (default)
        config = BaseConfig().cfg

        # Load specific files only
        config = BaseConfig(files=['config.yml', 'common.yml']).cfg

        # Exclude specific files
        config = BaseConfig(exclude=['common.yml']).cfg

        # Use environment variable to specify files
        # Set CONFIG_FILES="config.yml,common.yml" before import
    """

    def __init__(self, files=None, exclude=None):
        """
        Initialize BaseConfig

        Args:
            files: List of specific config files to load
                   (e.g., ['config.yml', 'common.yml'])
                   If None, loads all YAML files in the directory
            exclude: List of config files to exclude
                     (e.g., ['common.yml'])
        """
        self.current_path = os.path.dirname(os.path.abspath(__file__))
        self.cfg = {}

        # Find YAML files to load
        if files:
            # Load only specified files
            config_files = [
                f for f in files
                if os.path.exists(os.path.join(self.current_path, f))
            ]
        else:
            # Load all YAML files, excluding specified ones
            all_files = [
                file for file in os.listdir(self.current_path)
                if file.endswith('.yml') or file.endswith('.yaml')
            ]
            if exclude:
                config_files = [f for f in all_files if f not in exclude]
            else:
                config_files = all_files

        # Load and merge configuration files
        for config_file in sorted(config_files):
            config_file_path = os.path.join(self.current_path, config_file)
            try:
                with open(config_file_path, 'r', encoding='utf-8') as file:
                    file_cfg = yaml.safe_load(file) or {}
                    # Merge configurations (later files override earlier ones)
                    self.cfg = {**self.cfg, **file_cfg}
            except (IOError, yaml.YAMLError) as e:
                msg = f"Warning: Failed to load config file {config_file}: {e}"
                print(msg)


# Create a default config instance for easy access (loads all files)
base_config = BaseConfig().cfg
