import asyncio
import time
from typing import Optional

from loguru import logger
from pydantic import BaseModel

from ..celery_app import celery
from ..database import async_session_maker
from ..manager.crud import FileDAO
from ..manager.models import FileStatus
from ..services.sftp import SFTPService


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


def set_status(server_id, filename, size, status, error_message=None):
    update_file_status(
        DownloadedFileUpdateStatus(
            server_id=server_id,
            filename=filename,
            size=size,
            status=status,
            error_message=error_message,
        )
    )


@celery.task(bind=True, max_retries=10)
def download_file_task(
    self,
    host: str,
    port: int,
    username: str,
    password: str,
    remote_path: str,
    file: dict,
    server_id: int,
):
    filename = file.get("filename", "неизвестный файл")
    file_size_byte = file.get("st_size", 0)
    file_size_mb = file_size_byte / 1024 / 1024

    logger.info(
        f"⬇️ Задача скачивания {filename} ({file_size_mb:.2f} МБ) с {host}:{remote_path}"
    )
    set_status(server_id, filename, file_size_byte, FileStatus.NEW.value)

    sftp_service = SFTPService(host, port, username, password)
    result = {
        "success": False,
        "filename": filename,
        "server": host,
        "path": remote_path,
        "size": file_size_byte,
        "server_id": server_id,
    }

    try:
        sftp_service.connect()
        logger.debug(f"🔌 Подключение к {host} установлено для загрузки {filename}")

        if not sftp_service.file_is_stable(remote_path, file):
            logger.warning(
                f"⚠️ Файл {filename} нестабилен, будет повторная попытка позже"
            )
            set_status(
                server_id,
                filename,
                file_size_byte,
                FileStatus.RETRY.value,
                "Файл нестабилен",
            )
            raise Exception("Файл нестабилен")

        set_status(server_id, filename, file_size_byte, FileStatus.DOWNLOADING.value)

        if sftp_service.download_file(remote_path, file):
            set_status(
                server_id,
                filename,
                file_size_byte,
                FileStatus.DOWNLOADED_TO_SERVER.value,
            )
            result["success"] = True
            elapsed_time = (
                time.time() - self.request.time_start
                if hasattr(self.request, "time_start")
                else time.time()
            )
            download_speed = file_size_mb / elapsed_time if elapsed_time > 0 else 0
            logger.info(
                f"✅ Загружен {filename} "
                f"({file_size_mb:.2f} МБ, {download_speed:.2f} МБ/сек)"
            )
        else:
            set_status(
                server_id,
                filename,
                file_size_byte,
                FileStatus.RETRY.value,
                "Ошибка при скачивании файла",
            )
            logger.error(f"❌ Не удалось загрузить файл {filename}")
            raise Exception("Ошибка при скачивании файла")

    except Exception as e:
        logger.error(f"❌ Ошибка при скачивании {filename}: {str(e)}")
        set_status(server_id, filename, file_size_byte, FileStatus.ERROR.value, str(e))
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка для {filename}")
            raise self.retry(countdown=60, exc=e)
    finally:
        try:
            sftp_service.disconnect()
            logger.debug(f"🔌 Соединение с {host} закрыто после загрузки {filename}")
        except Exception as disconnect_err:
            logger.warning(f"⚠️ Проблема при закрытии соединения: {str(disconnect_err)}")

    return result
