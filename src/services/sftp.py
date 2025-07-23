import json

from loguru import logger
from paramiko import AutoAddPolicy
from paramiko.client import SSHClient
from redis import Redis

from ..config import settings


# Сервис для мониторинг SFTP-серверов на наличие новых файлов
class SFTPService:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = SSHClient()
        self.sftp = None
        self.redis = Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
        )

    def connect(self):
        logger.debug(f"Подключение к SFTP-серверу {self.host}:{self.port}")
        try:
            self.client.load_system_host_keys()
            self.client.set_missing_host_key_policy(AutoAddPolicy())
            self.client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )
            self.sftp = self.client.open_sftp()
        except Exception as e:
            logger.error(f"Ошибка подключения к SFTP-серверу: {e}")

    def disconnect(self) -> None:
        logger.info(f"Отключение от SFTP-сервера {self.host}:{self.port}")
        try:
            if self.sftp:
                self.sftp.close()
            if self.client:
                self.client.close()
            logger.info("Отключение выполнено")
        except Exception as e:
            logger.warning(f"Ошибка при отключении: {e}")

    def _get_cached_files(self, host: str, path: str) -> dict:
        key = f"{host}:{path}"
        cache_data = self.redis.hgetall(key)
        if cache_data:
            # Десериализация значений
            result = {k.decode(): json.loads(v) for k, v in cache_data.items()}
            logger.debug(f"Кэш найден для ключа {key}")
            return result
        logger.debug(f"Кэш не найден для ключа {key}")
        return {}

    def _save_files_to_cache(self, host: str, path: str, files: list) -> None:
        key = f"{host}:{path}"
        files_to_save = {
            file.filename: json.dumps({"size": file.st_size, "mtime": file.st_mtime})
            for file in files
        }
        if files_to_save:
            self.redis.hset(key, mapping=files_to_save)
            logger.info(f"Сохранено {len(files_to_save)} файлов в кэш для {key}")

    def _get_new_or_changed_files(self, files: list, cached_files: dict) -> list:
        new_or_changed_files = []
        for file in files:
            cached = cached_files.get(file.filename)
            if (
                not cached
                or cached["size"] != file.st_size
                or cached["mtime"] != file.st_mtime
            ):
                new_or_changed_files.append(file)
        return new_or_changed_files

    def scan_directory(self, path: str) -> list:
        if not self.sftp:
            logger.error("SFTP-соединение не установлено")
            return []
        logger.info(f"Сканирование директории {path} на сервере {self.host}")
        try:
            files = self.sftp.listdir_attr(path)
            logger.info(f"Найдено файлов: {len(files)}")
            if cached_files := self._get_cached_files(self.host, path):
                new_files = self._get_new_or_changed_files(files, cached_files)
                if new_files:
                    logger.info(f"Найдено новых/изменённых файлов: {len(new_files)}")
                self._save_files_to_cache(self.host, path, files)
                return new_files
            else:
                logger.info("Файлов в кэше не найдено, сохраняем все")
            self._save_files_to_cache(self.host, path, files)
            return files
        except Exception as e:
            logger.error(f"Ошибка при сканировании директории {path}: {e}")
            return []
