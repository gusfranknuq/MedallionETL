from __future__ import annotations

import logging


def configure_project_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(message)s")
    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("pyspark").setLevel(logging.WARN)
    logging.getLogger("pyspark.sql.connect").setLevel(logging.WARN)
