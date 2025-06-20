import os
import random
import time

import requests

from src.util.config import CLASH_GROUP, CLASH_CONTROL_URL, CLASH_SECRET, CLASH_NODE_NAMES
from src.util.logger import logger

# 配置你的分组名和节点名
GROUP = CLASH_GROUP

nodes_env = os.getenv("NODES")
NODES = CLASH_NODE_NAMES

CONTROLLER = f"{CLASH_CONTROL_URL}/proxies/{CLASH_GROUP}"
SECRET = CLASH_SECRET


def main():
    i = 0
    while True:
        node = random.choice(NODES)
        url = CONTROLLER
        if SECRET:
            url += "?secret=" + SECRET
        resp = requests.put(url, json={"name": node})
        logger.info(f"Switched to {node}, status: {resp.status_code}")
        time.sleep(1)

if __name__ == "__main__":
    main()