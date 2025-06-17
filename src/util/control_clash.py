import random
import time

import requests

from src.util.config import CLASH_GROUP, CLASH_CONTROL_URL, CLASH_SECRET
from src.util.logger import logger

# 配置你的分组名和节点名
GROUP = CLASH_GROUP

NODES = [
"\U0001F1ED\U0001F1F0 香港 Bage | 东京云", "\U0001F1ED\U0001F1F0 香港 Bage | 港云", "\U0001F1ED\U0001F1F0 香港 Bage | 深云", "\U0001F1ED\U0001F1F0 香港 Bage | 华中", "\U0001F1ED\U0001F1F0 香港 Lite | 东京云", "\U0001F1ED\U0001F1F0 香港 Lite | 港云", "\U0001F1ED\U0001F1F0 香港 Lite | 深云", "\U0001F1ED\U0001F1F0 香港 Lite | 华中", "\U0001F1ED\U0001F1F0 香港 Lumen | 东京云", "\U0001F1ED\U0001F1F0 香港 Lumen | 港云", "\U0001F1ED\U0001F1F0 香港 Lumen | 深云", "\U0001F1ED\U0001F1F0 香港 Lumen | 华中", "\U0001F1ED\U0001F1F0 香港 One | 港云", "\U0001F1ED\U0001F1F0 香港 One | 深云", "\U0001F1ED\U0001F1F0 香港 One | 华北", "\U0001F1ED\U0001F1F0 香港 One | 华中", "\U0001F1ED\U0001F1F0 香港 PCCW | 港云", "\U0001F1ED\U0001F1F0 香港 PCCW | 深云", "\U0001F1ED\U0001F1F0 香港 PCCW | 华中", "\U0001F1EF\U0001F1F5 日本 BGP | 直连", "\U0001F1EF\U0001F1F5 日本 BGP | 东京云", "\U0001F1EF\U0001F1F5 日本 BGP | 港云", "\U0001F1EF\U0001F1F5 日本 BGP | 深云", "\U0001F1EF\U0001F1F5 日本 BGP | 华东", "\U0001F1EF\U0001F1F5 日本 IIJ | 直连", "\U0001F1EF\U0001F1F5 日本 IIJ | 东京云", "\U0001F1EF\U0001F1F5 日本 IIJ | 港云", "\U0001F1EF\U0001F1F5 日本 IIJ | 深云", "\U0001F1EF\U0001F1F5 日本 IIJ | 华东", "\U0001F1EF\U0001F1F5 日本 软银 | 直连", "\U0001F1EF\U0001F1F5 日本 软银 | 港云", "\U0001F1EF\U0001F1F5 日本 软银 | 深云", "\U0001F1F8\U0001F1EC 新加坡 BGP | 东京云", "\U0001F1F8\U0001F1EC 新加坡 BGP | 华中", "\U0001F1F8\U0001F1EC 新加坡 Lite | 东京云", "\U0001F1F8\U0001F1EC 新加坡 Lite | 港云", "\U0001F1F8\U0001F1EC 新加坡 Lite | 深云", "\U0001F1F8\U0001F1EC 新加坡 Lite | 华中", "\U0001F1F9\U0001F1FC 台湾 Lite | 东京云", "\U0001F1F9\U0001F1FC 台湾 Lite | 港云", "\U0001F1F9\U0001F1FC 台湾 Lite | 深云", "\U0001F1F9\U0001F1FC 台湾 Lite | 华东", "\U0001F1F9\U0001F1FC 台湾 Lite | 华中", "\U0001F1F9\U0001F1FC 台湾 台中 | 直连", "\U0001F1F9\U0001F1FC 台湾 台中 | 东京云", "\U0001F1F9\U0001F1FC 台湾 台中 | 港云", "\U0001F1F9\U0001F1FC 台湾 台中 | 深云", "\U0001F1F9\U0001F1FC 台湾 台中 | 华东", "\U0001F1F9\U0001F1FC 台湾 台中 | 华中", "\U0001F1F9\U0001F1FC 台湾 彰化 | 直连", "\U0001F1F9\U0001F1FC 台湾 彰化 | 东京云", "\U0001F1F9\U0001F1FC 台湾 彰化 | 港云", "\U0001F1F9\U0001F1FC 台湾 彰化 | 深云", "\U0001F1F9\U0001F1FC 台湾 彰化 | 华东", "\U0001F1F9\U0001F1FC 台湾 彰化 | 华中", "\U0001F1FA\U0001F1F8 美国 2 | 直连", "\U0001F1FA\U0001F1F8 美国 2 | 东京云", "\U0001F1FA\U0001F1F8 美国 2 | 港云", "\U0001F1FA\U0001F1F8 美国 2 | 深云", "\U0001F1FA\U0001F1F8 美国 CU | 直连", "\U0001F1FA\U0001F1F8 美国 CU | 东京云", "\U0001F1FA\U0001F1F8 美国 CU | 港云", "\U0001F1FA\U0001F1F8 美国 CU | 深云", "\U0001F1FA\U0001F1F8 美国 CU | 华东", "\U0001F1FA\U0001F1F8 美国 IT7 | 直连", "\U0001F1FA\U0001F1F8 美国 IT7 | 东京云", "\U0001F1FA\U0001F1F8 美国 IT7 | 港云", "\U0001F1FA\U0001F1F8 美国 IT7 | 深云", "\U0001F1FA\U0001F1F8 美国 IT7 | 华东", "\U0001F1EB\U0001F1F7 法国 | 东京云", "\U0001F1EB\U0001F1F7 法国 | 亚欧", "\U0001F1EB\U0001F1F7 法国 | 港云", "\U0001F1EB\U0001F1F7 法国 | 华北", "\U0001F1EB\U0001F1F7 法国 | 华东", "\U0001F1EB\U0001F1F7 法国 | 华中", "\U0001F1E6\U0001F1FA 悉尼 Pro | 直连", "\U0001F1E6\U0001F1FA 悉尼 Pro | 港云", "\U0001F1E6\U0001F1FA 悉尼 Pro | 深云", "\U0001F1E6\U0001F1FA 悉尼 Pro | 华中", "\U0001F1F5\U0001F1F1 波兰 | 东京云", "\U0001F1F5\U0001F1F1 波兰 | 亚欧", "\U0001F1F5\U0001F1F1 波兰 | 港云", "\U0001F1F5\U0001F1F1 波兰 | 华北", "\U0001F1F5\U0001F1F1 波兰 | 华东", "\U0001F1F5\U0001F1F1 波兰 | 华中", "\U0001F1F3\U0001F1F1 荷兰 | 东京云", "\U0001F1F3\U0001F1F1 荷兰 | 亚欧", "\U0001F1F3\U0001F1F1 荷兰 | 港云", "\U0001F1F3\U0001F1F1 荷兰 | 华北", "\U0001F1F3\U0001F1F1 荷兰 | 华东", "\U0001F1F3\U0001F1F1 荷兰 | 华中"
]
# NODES = [
#     'HongKong-IPLC-HK-1-Rate:1.0','HongKong-IPLC-HK-2-Rate:1.0','HongKong-IPLC-HK-3-Rate:1.0','HongKong-IPLC-HK-4-Rate:1.0','HongKong-IPLC-HK-5-Rate:1.0','HongKong-IPLC-HK-6-Rate:1.0','HongKong-IPLC-HK-7-Rate:1.0','HongKong-IPLC-HK-8-Rate:1.0','HongKong-IPLC-HK-9-Rate:1.0','HongKong-IPLC-HK-10-Rate:1.0','HongKong-IPLC-HK-11-Rate:1.0','Japan-FW-JP-TEST1-Rate:1.0','Japan-FW-JP-TEST2-Rate:1.0','Japan-FW-JP1-Rate:1.0','Japan-FW-JP2-Rate:1.0','UnitedStates-FW-US1-Rate:1.5','UnitedStates-FW-US2-Rate:1.5','UnitedStates-FW-US3-Rate:1.0','Australia-FW-AU1-Rate:1.0','UnitedKingdom-FW-UK1-Rate:1.0','Estonia-EE-1-Rate:0.1','Australia-AU-1-Rate:0.5','Australia-AU-2-Rate:1.0','Australia-AU-3-Rate:1.0','Australia-AU-4-Rate:1.0','Germany-DE-2-Rate:1.0','Germany-DE-3-Rate:1.0','UnitedStates-US-1-Rate:1.5','UnitedStates-US-2-Rate:1.0','UnitedStates-US-3-Rate:1.0','HongKong-HK-1-Rate:1.0','HongKong-HK-2-Rate:1.0','Singapore-SG-1-Rate:1.0','Singapore-SG-2-Rate:1.0','Japan-TY-1-Rate:1.0','Japan-TY-2-Rate:1.0','Japan-TY-3-Rate:1.0','Japan-TY-4-Rate:1.0','Japan-OS-1-Rate:1.0','Japan-OS-2-Rate:1.0','Japan-OS-3-Rate:1.0','Netherlands-NL-1-Rate:1.0','Netherlands-NL-2-Rate:1.0','Netherlands-NL-3-Rate:1.0','UnitedKingdom-UK-1-Rate:1.0','Taiwan-TW-1-Rate:1.0','Taiwan-TW-2-Rate:1.0','UnitedStates-V6-US3-Rate:1.0','Netherlands-V6-NL1-Rate:1.0'
# ]
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