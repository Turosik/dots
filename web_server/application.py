import asyncio
import json
import time
from datetime import datetime
from decimal import Decimal
from json import JSONDecodeError
from logging import INFO, ERROR, WARNING

import aiohttp
from aiohttp import web, WSMessage

from database_operations.db import symbols_get_all, dots_insert
from database_operations.init_db import check_db_structure
from database_operations.utils import create_engine_async, close_engine_async
from settings.utils import convert_parameter_string_to_list
from web_server.subscriber import Subscriber
from web_server.utils import format_decimal


class WebServer(web.Application):
    index = 0
    db_engine = None
    symbols = {}
    background_task = None
    subscribers = set()

    def __init__(self, *args, app_logger, config_file, **kwargs):
        self.name = 'APP'
        self.config = config_file
        self.app_logger = app_logger
        self.decimals = config_file['data_source'].getint('decimals')
        self.source_url = self.config['data_source']['url']
        self.refresh_rate = self.config['data_source'].getint('refresh_rate')

        super().__init__(*args, **kwargs)

        self.router.add_route('GET', '/', self.websocket_handler)

        self.on_startup.append(self.app_init)
        self.on_startup.append(self.start_background_tasks)
        self.on_shutdown.append(self.disconnect_subscribers)
        self.on_shutdown.append(self.stop_background_tasks)
        self.on_cleanup.append(self.app_cleanup)

    # noinspection PyUnusedLocal
    async def app_init(self, *args):
        self.log(INFO, 'Checking DB structure...')
        check_db_structure(
            self.config['postgres'],
            convert_parameter_string_to_list(self.config['data_source']['symbols']),
            self.log)
        self.log(INFO, 'Checking DB structure COMPLETE!!!')
        self.db_engine = await create_engine_async(self.config['postgres'])
        self.log(INFO, 'Database connections READY!!! {}'.format(self.db_engine))
        self.symbols = {row.symbol: row.id for row in await symbols_get_all(self.db_engine)}

    # noinspection PyUnusedLocal
    async def app_cleanup(self, *args):
        await close_engine_async(self.db_engine)
        self.log(INFO, 'Database connections CLOSED!!!')

    # noinspection PyUnusedLocal
    async def start_background_tasks(self, *args):
        self.background_task = asyncio.create_task(self.prices_collector())

    # noinspection PyUnusedLocal
    async def stop_background_tasks(self, *args):
        if self.background_task:
            self.background_task.cancel()
            await self.background_task

    # noinspection PyUnusedLocal
    async def disconnect_subscribers(self, *args):
        await asyncio.gather(*[subscriber.disconnect() for subscriber in self.subscribers])

    def log(self, level, message, trace=False):
        self.app_logger.log(level, message, extra={'proc': self.name}, exc_info=trace if level >= ERROR else None)

    async def prices_collector(self):
        self.log(INFO, 'Prices collector started')
        async with aiohttp.ClientSession() as session:
            while True:
                start = time.time()
                dots_data = []
                notify_tasks = []
                try:
                    async with session.get(self.source_url) as response:
                        if response.status == 200:
                            response_text = await response.text()
                            response_text = response_text.replace('null', '')
                            response_text = response_text.replace('(', '')
                            response_text = response_text.replace(')', '')
                            response_text = response_text.replace(';', '')
                            price_data = json.loads(response_text)
                            for price in price_data['Rates']:
                                if price['Symbol'].upper() in self.symbols.keys():
                                    dots_data.append({
                                        'symbol_id': self.symbols[price['Symbol'].upper()],
                                        'value': Decimal((price['Bid'] + price['Ask']) / 2)
                                    })
                                    for subscriber in self.subscribers:
                                        if subscriber.subscription == self.symbols[price['Symbol'].upper()]:
                                            notify_tasks.append(subscriber.notify({
                                                'message': {
                                                    'assetName': price['Symbol'].upper(),
                                                    'time': int(datetime.utcnow().timestamp()),
                                                    'assetId': self.symbols[price['Symbol']],
                                                    'value': format_decimal(Decimal((price['Bid'] + price['Ask']) / 2),
                                                                            self.decimals)
                                                }
                                            }))
                        else:
                            self.log(ERROR, f'Response status {response.status} in prices_collector')

                    if len(dots_data) > 0:
                        await dots_insert(self.db_engine, dots_data)

                    if len(notify_tasks) > 0:
                        await asyncio.gather(*notify_tasks)

                except asyncio.CancelledError:
                    self.log(INFO, 'Prices collector cancelling')
                    break
                except Exception as error:
                    self.log(ERROR, f'Error occurred {error.__class__.__name__} in prices_collector')

                try:
                    await asyncio.sleep(self.refresh_rate - (time.time() - start))
                except asyncio.CancelledError:
                    self.log(INFO, 'Prices collector cancelling')
                    break
        self.log(INFO, 'Prices collector cancelled')

    async def websocket_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Add the new connection to the set of connected websockets
        subscriber = Subscriber(self, ws)
        self.subscribers.add(subscriber)

        self.log(INFO, 'New client connected')

        async for msg in ws:
            if isinstance(msg, WSMessage):
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        message_data = msg.json()
                        self.log(INFO, f'Received message: {message_data}')

                        if 'action' not in message_data:
                            await subscriber.send_message('Action not found')
                            continue

                        if message_data['action'] == 'assets':
                            await subscriber.assets()

                        elif message_data['action'] == 'subscribe':
                            if 'message' not in message_data:
                                await subscriber.send_message('Message not found')
                                continue

                            message = message_data['message']
                            if not isinstance(message_data['message'], dict):
                                await subscriber.send_message('Invalid message')
                                continue

                            if 'assetId' not in message:
                                await subscriber.send_message('Asset ID not found in message')
                                continue

                            if not isinstance(message['assetId'], int):
                                await subscriber.send_message('Asset ID must be an integer')
                                continue

                            print(message['assetId'])
                            if message['assetId'] not in self.symbols.values():
                                await subscriber.send_message('Asset ID does not exist')
                                continue

                            await subscriber.subscribe(message['assetId'])

                        else:
                            await subscriber.send_message('Action not supported')

                    except JSONDecodeError:
                        self.log(WARNING, f'Invalid JSON in message: {msg}')
                        await subscriber.send_message('Invalid JSON in message')
                    except Exception as error:
                        self.log(ERROR, f'Error {error.__class__.__name__} occurred processing message: {msg}')
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    await ws.close()
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.log(INFO, f'WebSocket connection closed with exception {ws.exception()}')
            else:
                self.log(WARNING, f'Unknown websocket message {msg}')

        # Remove the connection from the set when it's closed
        self.subscribers.remove(subscriber)
        del subscriber
        self.log(INFO, 'WebSocket connection closed')
        return ws

