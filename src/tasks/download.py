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


class DownloadedFileUpdateStatus(BaseModel):
    server_id: int
    filename: str
    size: float


class NewDownloadedFile(DownloadedFileUpdateStatus):
    status: FileStatus
    minio_path: Optional[str] = None
    error_message: Optional[str] = None


def update_downloaded_file_info(
    server_id,
    filename,
    size,
    status=FileStatus.NEW.value,
    minio_path=None,
    error_message=None,
    new_status=None,
):
    """Обновляет информацию о загруженном файле в базе данных"""
    try:

        async def inner():
            try:
                async with async_session_maker() as session:
                    if new_status:
                        file = await FileDAO().find_one_or_none(
                            session=session,
                            filters=DownloadedFileUpdateStatus(
                                server_id=server_id, filename=filename, size=size
                            ),
                        )
                        file.status = new_status
                        session.add(file)
                    else:
                        await FileDAO().add(
                            session=session,
                            values=NewDownloadedFile(
                                server_id=server_id,
                                filename=filename,
                                size=size,
                                status=status,
                                minio_path=minio_path,
                                error_message=error_message,
                            ),
                        )
            except Exception as e:
                logger.error(f"❌ Ошибка при обновлении информации о файле: {str(e)}")
                return []
            finally:
                await session.commit()

        return asyncio.run(inner())
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске асинхронной функции: {str(e)}")
        return []


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
    start_time = time.time()
    filename = file.get("filename", "неизвестный файл")
    file_size_mb = file.get("st_size", 0) / 1024 / 1024

    logger.info(
        f"⬇️ Запуск задачи скачивания {filename} ({file_size_mb} МБ) с {host}:{remote_path}"
    )
    update_downloaded_file_info(
        server_id=server_id, filename=filename, size=file_size_mb
    )

    sftp_service = SFTPService(
        host=host,
        port=port,
        username=username,
        password=password,
    )

    result = {
        "success": False,
        "filename": filename,
        "server": host,
        "path": remote_path,
        "size": file.get("st_size", 0),
        "server_id": server_id,
    }

    try:
        # Подключаемся к SFTP
        sftp_service.connect()
        logger.debug(f"🔌 Подключение к {host} установлено для загрузки {filename}")

        # Проверяем стабильность файла
        if not sftp_service.file_is_stable(remote_path, file):
            logger.warning(
                f"⚠️ Файл {filename} нестабилен, будет повторная попытка позже"
            )
            self.retry(countdown=30)
            return result

        # Скачиваем файл
        update_downloaded_file_info(
            server_id=server_id,
            filename=filename,
            size=file_size_mb,
            status=FileStatus.DOWNLOADING.value,
        )
        download_success = sftp_service.download_file(remote_path, file)

        if download_success:
            update_downloaded_file_info(
                server_id=server_id,
                filename=filename,
                size=file_size_mb,
                status=FileStatus.UPLOADED.value,
            )
            result["success"] = True

            elapsed_time = time.time() - start_time
            download_speed = file_size_mb / elapsed_time if elapsed_time > 0 else 0

            logger.info(
                f"✅ Успешно загружен {filename} "
                f"({file_size_mb:.2f} МБ за {elapsed_time:.1f} сек, "
                f"{download_speed:.2f} МБ/сек)"
            )
        else:
            update_downloaded_file_info(
                server_id=server_id,
                filename=filename,
                size=file_size_mb,
                status=FileStatus.RETRY.value,
                error_message="Ошибка при скачивании файла",
            )
            logger.error(f"❌ Не удалось загрузить файл {filename}")
            if self.request.retries < self.max_retries:
                logger.info(f"🔄 Планирование повторной попытки для {filename}")
                self.retry(countdown=60)

    except Exception as e:
        logger.error(f"❌ Ошибка при скачивании {filename}: {str(e)}")
        result["error"] = str(e)
        update_downloaded_file_info(
            server_id=server_id,
            filename=filename,
            size=file_size_mb,
            status=FileStatus.ERROR.value,
            error_message=str(e),
        )
        # Повторяем задачу при ошибке
        if self.request.retries < self.max_retries:
            logger.info(
                f"🔄 Планирование повторной попытки после ошибки для {filename}"
            )
            self.retry(countdown=60, exc=e)

    finally:
        # Закрываем соединение
        try:
            sftp_service.disconnect()
            logger.debug(f"🔌 Соединение с {host} закрыто после загрузки {filename}")
        except Exception as disconnect_err:
            logger.warning(f"⚠️ Проблема при закрытии соединения: {str(disconnect_err)}")

    return result
