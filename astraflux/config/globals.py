# -*- encoding: utf-8 -*-


_YAML_PATH = None
_CURRENT_DIR = None


def set_yaml_path(path: str):
    global _YAML_PATH
    _YAML_PATH = path


def get_yaml_path() -> str | None:
    global _YAML_PATH
    return _YAML_PATH


def set_current_dir(path: str):
    global _CURRENT_DIR
    _CURRENT_DIR = path


def get_current_dir() -> str | None:
    global _CURRENT_DIR
    return _CURRENT_DIR
