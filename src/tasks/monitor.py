import asyncio
from collections import defaultdict

from loguru import logger
from pydantic import BaseModel

from ..celery_app import celery
from ..database import async_session_maker
from ..manager.crud import ServerDAO
from ..services.sftp import SFTPService


class ActiveServer(BaseModel):
    scanning: bool


def get_active_servers():
    async def inner():
        async with async_session_maker() as session:
            servers = await ServerDAO().find_all(
                session=session, filters=ActiveServer(scanning=True)
            )
            return servers

    return asyncio.run(inner())


@celery.task()
def scan_all_servers():
    total_errors = 0
    logger.info("Начало сканирования всех серверов.")
    if not (servers := get_active_servers()):
        logger.info("Нет активных серверов для сканирования.")
        return
    logger.info(f"Найдено {len(servers)} активных серверов для сканирования.")
    union_servers = defaultdict(list)
    for server in servers:
        union_servers[
            f"{server.host}:{server.port}:{server.username}:{server.password}"
        ].append(server.path)
    for connection_params, paths in union_servers.items():
        host, port, username, password = connection_params.split(":")
        sftp_service = SFTPService(
            host=host,
            port=int(port),
            username=username,
            password=password,
        )
        try:
            sftp_service.connect()
            logger.info(f"Сканирование сервера {host} начато.")
            for path in paths:
                files = sftp_service.scan_directory(path)
                # Здесь будет логика отправки задачи на скачивания файлов
                # чтото наподобие такого:
                # for file in files:
                #     download_file_task.apply_async(file, host, path, queue='download_queue')
        except Exception as e:
            logger.error(f"Ошибка при сканировании сервера {host}: {e}")
            total_errors += 1
        finally:
            sftp_service.disconnect()
            logger.info(f"Сканирование сервера {host} завершено.")
    return {"Проверено серверов": len(union_servers), "Ошибок": total_errors}
