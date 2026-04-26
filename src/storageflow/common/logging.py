import logging
import os
import json
from contextvars import ContextVar
import traceback

# Global context variable to store run_uuid per request/task
run_uuid_var: ContextVar[str | None] = ContextVar("run_uuid", default=None)


class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        run_uuid = run_uuid_var.get()
        if run_uuid:
            setattr(record, "run_uuid", run_uuid)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_record_dict = {
            "severity": record.levelname,
            "timestamp": self.formatTime(record),
            "message": record.getMessage(),
            "run_uuid": getattr(record, "run_uuid", None),
        }

        # Include stack trace if exception info is present
        if record.exc_info:
            log_record_dict["stack_trace"] = "".join(
                traceback.format_exception(*record.exc_info)
            )

        for key, value in record.__dict__.items():
            if key not in [
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "asctime",
                "run_uuid",
            ]:
                log_record_dict[key] = value

        log_record_dict = {k: v for k, v in log_record_dict.items() if v is not None}

        return json.dumps(log_record_dict)


def get_logger(name: str = "storageflow", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        request_context_filter = RequestContextFilter()

        if os.getenv("LOGGING_ENV") == "GCP":
            # For environments that use the logging agent to parse stdout
            handler = logging.StreamHandler()
            handler.setFormatter(JsonFormatter())
            handler.addFilter(request_context_filter)
            logger.addHandler(handler)
        else:
            # For local development
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(run_uuid)s"
            )
            handler.setFormatter(formatter)
            handler.addFilter(request_context_filter)
            logger.addHandler(handler)

    return logger
