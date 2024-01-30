from datetime import datetime, timedelta

import sqlalchemy as sa
from sqlalchemy import (
    Column, SmallInteger, DECIMAL,
    Integer, String, DateTime, ForeignKey
)
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Symbols(Base):
    # noinspection SpellCheckingInspection
    __tablename__ = 'symbols'

    id = Column(SmallInteger, primary_key=True)
    symbol = Column(String(32), nullable=False, unique=True, index=True)


class Dots(Base):
    # noinspection SpellCheckingInspection
    __tablename__ = 'dots'

    id = Column(Integer, primary_key=True)
    symbol_id = Column(ForeignKey('symbols.id', ondelete='CASCADE'), nullable=False, index=True)
    value = Column(DECIMAL, nullable=False)
    timestamp = Column(DateTime, nullable=False, server_default=func.now(), index=True)


def symbols_add_if_not_exists_sync(engine, symbol):
    with engine.begin() as conn:
        _ = conn.execute(
            insert(Symbols)
            .values(symbol=symbol)
            .on_conflict_do_nothing()
        )
        conn.commit()


async def symbols_get_all(engine):
    async with engine.begin() as conn:
        return await conn.execute(sa.select(Symbols))


async def dots_insert(engine, dots_data):
    async with engine.begin() as conn:
        await conn.execute(insert(Dots), dots_data)
        await conn.commit()


async def dots_get_recent(engine, symbol_id, minutes_back=30):
    minutes_ago = datetime.utcnow() - timedelta(minutes=minutes_back)
    async with engine.connect() as conn:
        return await conn.execute(
            sa.select(
                Symbols.symbol,
                Dots.timestamp,
                Dots.value
            )
            .select_from(sa.join(Symbols, Dots, Dots.symbol_id == Symbols.id))
            .where(sa.and_(Symbols.id == symbol_id, Dots.timestamp >= minutes_ago))
            .order_by(sa.asc(Dots.timestamp))
        )
