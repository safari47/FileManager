from enum import Enum

from sqlalchemy import BigInteger, Boolean, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ..database import Base


# Enum для статуса файла
class FileStatus(Enum):
    NEW = "new" # Новый файл
    DOWNLOADING = "downloading" # Загружается
    UPLOADED = "uploaded" # Загружен
    ERROR = "error" # Ошибка
    RETRY = "retry" # Повторная попытка

# Модель для хранения информации о серверах и связи с файлами
class Server(Base):
    host: Mapped[str] = mapped_column(String,nullable=False) # Хост сервера
    port: Mapped[int] = mapped_column(Integer,nullable=False) # Порт сервера
    path: Mapped[str] = mapped_column(String, nullable=False) # Путь к папке на сервере для сканирования
    username: Mapped[str] = mapped_column(String, nullable=False) # Имя пользователя для доступа к серверу
    password: Mapped[str] = mapped_column(String, nullable=False) # Пароль для доступа к серверу
    scanning: Mapped[bool] = mapped_column(Boolean, default=True) # Флаг, указывающий на то, нужно ли сканировать сервер

# Модель для хранения информации о файлах
class File(Base):
    server_id: Mapped[int] = mapped_column(Integer, nullable=False) # ID сервера, на котором хранится файл
    filename: Mapped[str] = mapped_column(String, nullable=False) # Имя файла
    status: Mapped[FileStatus] = mapped_column(String, nullable=False, default=FileStatus.NEW) # Статус файла
    size: Mapped[int] = mapped_column(BigInteger, nullable=False) # Размер файла в байтах
    minio_path: Mapped[str] = mapped_column(String, nullable=True) # Путь к файлу в MinIO (если используется)
    error_message: Mapped[str] = mapped_column(String, nullable=True) # Сообщение об ошибке (если есть)
