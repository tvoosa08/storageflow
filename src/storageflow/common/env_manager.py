import os
from storageflow.common.logging import get_logger

LOGGING_LEVEL = os.environ.get("LOGGING_LEVEL", "DEBUG")

log = get_logger(LOGGING_LEVEL)


def get_mandatory_env_var(env_var, default_value=None):
    value = os.environ.get(env_var, default_value)
    if not value:
        log.error(
            f"missing mandatory environment-variable {env_var} .",
            env_var,
            extra={"env_var": env_var},
        )
        raise ValueError(f"missing mandatory environment-variable {env_var} .")
    return value
