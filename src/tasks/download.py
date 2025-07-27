from datetime import date

from loguru import logger

from ..celery_app import celery
from ..manager.crud import FileDAO
from ..manager.models import FileStatus
from ..services.sftp import SFTPService
from .crud import set_status
from .upload import upload_file_to_minio


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
            upload_file_to_minio.apply_async(
                kwargs={
                    "server_id": server_id,
                    "filename": filename,
                    "file_size_byte": file_size_byte,
                    "local_path": f"{sftp_service.get_local_path(host, remote_path)}/{filename}",
                    "minio_path": f"{remote_path}/{date.today().isoformat()}/{filename}",
                    "bucket_name": f"server-{host.replace('.', '-')}",
                },
                queue="upload_queue",
            )
            logger.info(f"✅ Файл {filename} успешно загружен на сервер {host}")
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
