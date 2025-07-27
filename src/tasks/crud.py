import asyncio
from typing import Optional

from loguru import logger
from pydantic import BaseModel

from ..database import async_session_maker
from ..manager.crud import FileDAO


class DownloadedFile(BaseModel):
    server_id: int
    filename: str
    size: float


class DownloadedFileUpdateStatus(DownloadedFile):
    status: str
    minio_path: Optional[str] = None
    error_message: Optional[str] = None


def update_file_status(data: DownloadedFileUpdateStatus):
    async def inner():
        async with async_session_maker() as session:
            try:
                await FileDAO().upsert_file(
                    session=session,
                    filters=DownloadedFile(
                        server_id=data.server_id,
                        filename=data.filename,
                        size=data.size,
                    ),
                    values=data,
                )
                await session.commit()
            except Exception as e:
                logger.error(
                    f"Ошибка при обновлении статуса файла {data.filename}: {e}"
                )
                await session.rollback()
                return False

    asyncio.run(inner())


def set_status(
    server_id: int,
    filename: str,
    size: float,
    status: str,
    minio_path: Optional[str] = None,
    error_message: Optional[str] = None
) -> None:
    update_file_status(
        DownloadedFileUpdateStatus(
            server_id=server_id,
            filename=filename,
            size=size,
            status=status,
            minio_path=minio_path,
            error_message=error_message,
        )
    )
