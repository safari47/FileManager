import json
import time
from datetime import date
from pathlib import Path

from loguru import logger
from paramiko import AutoAddPolicy
from paramiko.client import SSHClient
from redis import Redis

from ..config import settings


# Сервис для мониторинга SFTP-серверов на наличие новых файлов
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
        logger.debug(f"🔒 Подключение к SFTP {self.host}:{self.port}")
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
            logger.info(f"✅ SFTP соединение с {self.host} установлено")
        except Exception as e:
            raise RuntimeError(f"Ошибка подключения к SFTP {self.host}: {e}")

    def disconnect(self) -> None:
        logger.debug(f"🔒 Отключение от SFTP {self.host}")
        try:
            if self.sftp:
                self.sftp.close()
            if self.client:
                self.client.close()
            logger.debug(f"✅ SFTP соединение с {self.host} закрыто")
        except Exception as e:
            raise RuntimeError(f"Ошибка при отключении от SFTP {self.host}: {e}")

    def _get_cached_files(self, host: str, path: str) -> dict:
        key = f"{host}:{path}"
        cache_data = self.redis.hgetall(key)
        if cache_data:
            result = {k.decode(): json.loads(v) for k, v in cache_data.items()}
            logger.debug(f"🔍 Кеш найден для {key}: {len(result)} файлов")
            return result
        logger.debug(f"🔍 Кеш не найден для {key}")
        return {}

    def _save_files_to_cache(self, host: str, path: str, files: list) -> None:
        key = f"{host}:{path}"
        files_to_save = {
            file.filename: json.dumps({"size": file.st_size, "mtime": file.st_mtime})
            for file in files
        }
        if files_to_save:
            self.redis.hset(key, mapping=files_to_save)
            # Устанавливаем TTL для кеша (7 дней)
            self.redis.expire(key, 604800)
            logger.debug(f"💾 Кеш обновлен для {key}: {len(files_to_save)} файлов")

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

        if new_or_changed_files:
            logger.debug(
                f"🆕 Обнаружены изменения в {len(new_or_changed_files)} файлах"
            )
        return new_or_changed_files

    def scan_directory(self, path: str) -> list:
        if not self.sftp:
            raise RuntimeError(
                f"SFTP соединение не установлено при сканировании {path}"
            )

        logger.info(f"🔍 Сканирование {self.host}:{path}")
        try:
            files = self.sftp.listdir_attr(path)
            if not files:
                logger.info(f"📂 Директория {path} пуста")
                return []

            logger.debug(f"📊 Найдено {len(files)} файлов в {path}")
            cached_files = self._get_cached_files(self.host, path)

            if cached_files:
                new_files = self._get_new_or_changed_files(files, cached_files)
                if new_files:
                    logger.info(f"🆕 {len(new_files)} новых/измененных файлов в {path}")
                else:
                    logger.info(f"✅ Нет изменений в {path}")
                self._save_files_to_cache(self.host, path, files)
                return new_files
            else:
                logger.info(f"🆕 Первое сканирование {path}: {len(files)} файлов")
                self._save_files_to_cache(self.host, path, files)
                return files
        except Exception as e:
            raise RuntimeError(f"Ошибка сканирования {self.host}:{path}: {e}")

    @staticmethod
    def sftp_attr_to_dict(file):
        return {
            "filename": file.filename,
            "st_size": file.st_size,
            "st_mtime": file.st_mtime,
        }

    def get_local_path(self, host: str, remote_path: str) -> str:
        local_path = (
            Path(settings.LOCAL_DOWNLOAD_PATH)
            / host
            / remote_path.lstrip("/")
            / date.today().isoformat()
        )
        local_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"📁 Подготовлена директория для загрузки: {local_path}")
        return str(local_path)

    def file_is_stable(self, path: str, file: dict, sleep: int = 10) -> bool:
        filename = file["filename"]
        logger.debug(f"⏱️ Проверка стабильности файла {filename}")
        try:
            first_stat = self.sftp.stat(f"{path}/{filename}")
            time.sleep(sleep)
            second_stat = self.sftp.stat(f"{path}/{filename}")
            return (
                first_stat.st_size == second_stat.st_size
                and first_stat.st_mtime == second_stat.st_mtime
            )
        except Exception as e:
            raise RuntimeError(f"Ошибка проверки стабильности {filename}: {e}")

    def download_file(self, remote_path: str, file: dict) -> bool:
        if not self.sftp:
            raise RuntimeError("SFTP соединение не установлено")
        filename = file["filename"]
        try:
            local_path = self.get_local_path(self.host, remote_path)
            if not self.file_is_stable(path=remote_path, file=file):
                logger.warning(f"⚠️ Файл {filename} нестабилен, пропускаем")
                return False
            logger.info(
                f"⬇️ Загрузка: {remote_path}/{filename} → {local_path}/{filename}"
            )
            self.sftp.get(
                remotepath=f"{remote_path}/{filename}",
                localpath=f"{local_path}/{filename}",
            )
            logger.info(
                f"✅ Загружен файл {filename} ({file['st_size']/1024/1024:.2f} МБ)"
            )
            return True
        except Exception as e:
            raise RuntimeError(f"Ошибка загрузки {filename}: {e}")
