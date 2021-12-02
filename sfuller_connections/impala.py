import os
from typing import NamedTuple


class ImpalaConfig(NamedTuple):
    host: str
    port: int
    user: str
    password: str
    timeout: int
    auth_mechanism: str

try:
    ImpalaConfigFromEnv = ImpalaConfig(host=os.getenv("IMPALA_HOST"),
                                       port=int(os.getenv("IMPALA_PORT")),
                                       user=os.getenv("IMPALA_USER"),
                                       password=os.getenv("IMPALA_PASSWORD"),
                                       timeout=int(os.getenv("IMPALA_TIMEOUT")),
                                       auth_mechanism=os.getenv("IMPALA_AUTH_MECHANISM"))
except Error as e:
    print("problem setting up environmental variables")
    print("please refer to the environmental variable setup doc")
    print("https://captify.atlassian.net/wiki/spaces/PT/pages/520847628/Environmental+Variables")
    raise e
