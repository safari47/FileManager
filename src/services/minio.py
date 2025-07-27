from loguru import logger
from minio import Minio
from minio.error import S3Error

from ..config import settings


class MinioClient:
    def __init__(self):
        self.client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=False,
        )

    def upload_file(self, bucket_name: str, local_path: str, minio_path: str) -> None:
        # Проверяем, существует ли бакет
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
        # Загружаем файл по нужному пути
        try:
            self.client.fput_object(bucket_name, minio_path, local_path)
        except S3Error as err:
            logger.error(f"Ошибка загрузки файла в MinIO: {err}")
            raise
