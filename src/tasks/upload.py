from loguru import logger

from ..celery_app import celery
from ..manager.models import FileStatus
from ..services.minio import MinioClient
from .crud import set_status


@celery.task(bind=True, max_retries=10)
def upload_file_to_minio(
    self,
    server_id: int,
    filename: str,
    file_size_byte: float,
    local_path: str,
    minio_path: str,
    bucket_name: str,
):
    try:
        minio_service = MinioClient()
        minio_service.upload_file(
            bucket_name=bucket_name, local_path=local_path, minio_path=minio_path
        )
        logger.info(f"✅ Файл {filename} успешно загружен в MinIO по пути {minio_path}")
        set_status(
            server_id,
            filename,
            file_size_byte,
            FileStatus.DOWNLOADED_TO_MINIO.value,
            minio_path=f"{bucket_name}/{minio_path}",
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке файла {filename} в MinIO: {str(e)}")
        set_status(server_id, filename, file_size_byte, FileStatus.ERROR.value, str(e))
        if self.request.retries < self.max_retries:
            logger.info(f"🔄 Повторная попытка для {filename}")
            raise self.retry(countdown=60, exc=e)
