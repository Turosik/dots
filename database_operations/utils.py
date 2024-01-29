from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine


def create_engine_sync(config, echo=False):
    db_url = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**config)
    return create_engine(db_url, echo=echo)


async def create_engine_async(config, echo=False):
    db_url = 'postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}'.format(**config)
    return create_async_engine(url=db_url,
                               pool_recycle=config.getint('pool_recycle'),
                               echo=echo)


async def close_engine_async(engine):
    await engine.dispose(close=True)
