from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from loguru import logger

from .database import create_tables
from .manager.router import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Старт приложения...")
    logger.info(f"Создание таблиц базы данных")
    await create_tables()
    logger.info("Таблицы базы данных успешно созданы")
    yield
    logger.info("Остановка приложения...")


app = FastAPI(lifespan=lifespan, title="Api для мониторинга серверов")
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("main:app", reload=True)
