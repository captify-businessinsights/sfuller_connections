import os
from typing import NamedTuple


class ImpalaConfig(NamedTuple):
    host: str
    port: int
    user: str
    password: str
    timeout: int
    auth_mechanism: str


ImpalaConfigFromEnv = ImpalaConfig(host=os.getenv("IMPALA_HOST"),
                                   port=int(os.getenv("IMPALA_PORT")),
                                   user=os.getenv("IMPALA_USER"),
                                   password=os.getenv("IMPALA_PASSWORD"),
                                   timeout=int(os.getenv("IMPALA_TIMEOUT")),
                                   auth_mechanism=os.getenv("IMPALA_AUTH_MECHANISM"))

