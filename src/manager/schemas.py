from typing import Optional

from pydantic import BaseModel, Field


class ServerID(BaseModel):
    id: Optional[int] = None


class ServerSchema(ServerID):
    host: str = Field(..., description="Хост сервера", example="192.168.0.1")
    port: int = Field(..., description="Порт сервера", example=22)
    path: str = Field(..., description="Путь к папке на сервере для сканирования", example="/var/files")
    username: str = Field(..., description="Имя пользователя для доступа к серверу", example="admin")
    password: str = Field(..., description="Пароль для доступа к серверу", example="root1234")
    scanning: bool = Field(..., description="Флаг, указывающий на то, нужно ли сканировать сервер", example=True)


class FileSchema(BaseModel):
    id: Optional[int] = Field(
        None, description="Уникальный идентификатор файла", example=1
    )
    server_id: int = Field(..., description="ID сервера, на котором хранится файл", example=1)
    filename: str = Field(..., description="Имя файла", example="example.txt") 
    status: str = Field(..., description="Статус файла", example="new")
    size: float = Field(..., description="Размер файла в байтах", example=1024)
    minio_path: Optional[str] = Field(
        None,
        description="Путь к файлу в MinIO (если используется)",
        example="minio/bucket/example.txt",
    )
    error_message: Optional[str] = Field(
        None, description="Сообщение об ошибке (если есть)", example="File not found"
    )
