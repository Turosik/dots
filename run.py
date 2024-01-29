from aiohttp import web

from settings import CONFIG_FILE
from settings.logger import config_logger
from web_server.application import WebServer

log = config_logger(CONFIG_FILE, 'eo_trade_bots')

app = WebServer(
    config_file=CONFIG_FILE,
    app_logger=log
)

web.run_app(app, host=CONFIG_FILE['network']['host'], port=CONFIG_FILE['network'].getint('port'))
