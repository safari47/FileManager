from typing import Generic, List, Type, TypeVar

from loguru import logger
from pydantic import BaseModel
from sqlalchemy import delete as sqlalchemy_delete
from sqlalchemy import func
from sqlalchemy import update as sqlalchemy_update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from ..database import Base
from .models import File, Server

T = TypeVar("T", bound=Base)


class BaseDAO(Generic[T]):
    model: Type[T] = None

    async def find_one_or_none_by_id(self, session: AsyncSession, data_id: int):
        # Поиск одной записи по ID
        try:
            query = select(self.model).filter_by(id=data_id)
            result = await session.execute(query)
            record = result.scalar_one_or_none()
            log_message = f"Запись {self.model.__name__} с ID {data_id} {'найдена' if record else 'не найдена'}."
            logger.info(log_message)
            return record
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при поиске записи с ID {data_id}: {e}")
            raise

    async def delete_by_id(self, session: AsyncSession, data_id: int):
        # Удаление записи по ID
        try:
            query = sqlalchemy_delete(self.model).where(self.model.id == data_id)
            result = await session.execute(query)
            await session.commit()
            logger.info(f"Запись {self.model.__name__} с ID {data_id} успешно удалена.")
            return result.rowcount > 0
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при удалении записи с ID {data_id}: {e}")
            raise

    async def find_one_or_none(self, session: AsyncSession, filters: BaseModel):
        # Поиск одной записи по фильтрам
        filter_dict = filters.model_dump(exclude_unset=True)
        logger.info(
            f"Поиск одной записи {self.model.__name__} по фильтрам: {filter_dict}"
        )
        try:
            query = select(self.model).filter_by(**filter_dict)
            result = await session.execute(query)
            record = result.scalar_one_or_none()
            log_message = f"Запись {'найдена' if record else 'не найдена'} по фильтрам: {filter_dict}"
            logger.info(log_message)
            return record
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при поиске записи по фильтрам {filter_dict}: {e}")
            raise

    async def find_all(
        self,
        session: AsyncSession,
        filters: BaseModel | None = None,
        limit: int = None,
        offset: int = 0,
    ):
        # Поиск всех записей по фильтрам
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        logger.info(
            f"Поиск всех записей {self.model.__name__} по фильтрам: {filter_dict}"
        )
        try:
            query = (
                select(self.model)
                .filter_by(**filter_dict)
                .order_by(self.model.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            result = await session.execute(query)
            records = result.scalars().all()
            logger.info(f"Найдено {len(records)} записей.")
            return records
        except SQLAlchemyError as e:
            logger.error(
                f"Ошибка при поиске всех записей по фильтрам {filter_dict}: {e}"
            )
            raise

    async def count_all(self, session: AsyncSession, filters: BaseModel | None = None):
        filter_dict = filters.model_dump(exclude_unset=True) if filters else {}
        query = select(func.count()).select_from(self.model).filter_by(**filter_dict)
        result = await session.execute(query)
        return result.scalar_one()

    async def add(self, session: AsyncSession, values: BaseModel):
        # Добавление одной записи
        values_dict = values.model_dump(exclude_unset=True)
        logger.info(
            f"Добавление записи {self.model.__name__} с параметрами: {values_dict}"
        )
        try:
            new_instance = self.model(**values_dict)
            session.add(new_instance)
            logger.info(f"Запись {self.model.__name__} успешно добавлена.")
            await session.flush()
            return new_instance
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при добавлении записи: {e}")
            raise

    async def update(
        self, session: AsyncSession, filters: BaseModel, values: BaseModel
    ):
        # Обновление записей по фильтрам
        filter_dict = filters.model_dump(exclude_unset=True)
        values_dict = values.model_dump(exclude_unset=True)
        logger.info(
            f"Обновление записей {self.model.__name__} по фильтру: {filter_dict} с параметрами: {values_dict}"
        )
        try:
            query = (
                sqlalchemy_update(self.model)
                .where(*[getattr(self.model, k) == v for k, v in filter_dict.items()])
                .values(**values_dict)
                .execution_options(synchronize_session="fetch")
            )
            result = await session.execute(query)
            logger.info(f"Обновлено {result.rowcount} записей.")
            await session.flush()
            return result
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при обновлении записей: {e}")
            raise


class ServerDAO(BaseDAO):
    model = Server


class FileDAO(BaseDAO):
    model = File

    async def upsert_file(
        self, session: AsyncSession, filters: BaseModel, values: BaseModel
    ):
        file = await self.find_one_or_none(session=session, filters=filters)
        if file:
            await self.update(session=session, filters=filters, values=values)
        else:
            await self.add(session=session, values=values)
