from database_operations.db import dots_get_recent
from web_server.utils import format_decimal


class Subscriber:
    def __init__(self, app, connection, subscription=0):
        self.app = app
        self.connection = connection
        self.subscription = subscription

    async def assets(self):
        await self.connection.send_json({
            'action': 'assets',
            'message': {
                'assets': [{
                    'id': value,
                    'name': key
                } for key, value in self.app.symbols.items()]
            }
        })

    async def recent_dots(self):
        await self.connection.send_json({
            'action': 'asset_history',
            'message': {
                'points': [{
                    'assetName': dot.symbol,
                    'time': int(dot.timestamp.timestamp()),
                    'assetId': self.subscription,
                    'value': format_decimal(dot.value, self.app.decimals)
                } for dot in await dots_get_recent(self.app.db_engine, self.subscription)]
            }
        })

    async def subscribe(self, subscription: int):
        self.subscription = subscription
        await self.recent_dots()

    async def notify(self, message):
        await self.connection.send_json(message)

    async def send_message(self, message):
        await self.connection.send_str(message)

    async def disconnect(self):
        await self.connection.close()
