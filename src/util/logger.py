import logging
import os

# 日志格式，包含 py 文件名 (module) 和方法名 (funcName)
LOG_FORMAT = "%(asctime)s %(levelname)s [%(module)s.%(funcName)s] %(name)s: %(message)s"

# 日志级别（可通过环境变量控制，默认INFO）
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# 日志配置
logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler()
        # 如需写入文件可添加: logging.FileHandler("app.log")
    ]
)

# 推荐用法：统一导出一个 logger 实例
logger = logging.getLogger("soundcloud_project")