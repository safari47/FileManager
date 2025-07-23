import asyncio
from collections import defaultdict

from loguru import logger
from pydantic import BaseModel

from ..celery_app import celery
from ..database import async_session_maker
from ..manager.crud import ServerDAO
from ..services.sftp import SFTPService
from .download import download_file_task


class ActiveServer(BaseModel):
    scanning: bool


def get_active_servers():
    """Получает список активных серверов из базы данных"""
    try:

        async def inner():
            try:
                async with async_session_maker() as session:
                    servers = await ServerDAO().find_all(
                        session=session, filters=ActiveServer(scanning=True)
                    )
                    return servers
            except Exception as e:
                logger.error(f"❌ Ошибка при получении серверов из БД: {str(e)}")
                return []

        return asyncio.run(inner())
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске асинхронной функции: {str(e)}")
        return []


@celery.task()
def scan_all_servers():
    """Сканирует все активные серверы на наличие новых файлов"""
    total_errors = 0
    processed_servers = 0
    processed_files = 0

    logger.info("🚀 Запуск сканирования всех серверов")

    # Получаем список активных серверов
    servers = get_active_servers()
    if not servers:
        logger.info("ℹ️ Нет активных серверов для сканирования")
        return {"status": "success", "message": "Нет активных серверов"}

    logger.info(f"📊 Найдено {len(servers)} активных серверов для сканирования")

    # Группируем серверы по параметрам подключения
    union_servers = defaultdict(list)
    for server in servers:
        connection_key = (
            f"{server.host}:{server.port}:{server.username}:{server.password}"
        )
        union_servers[connection_key].append((server.id, server.path))

    # Обрабатываем каждую группу серверов
    for connection_params, server_paths in union_servers.items():
        try:
            # Разбираем параметры подключения
            host, port, username, password = connection_params.split(":")
            logger.debug(f"🔑 Подготовка подключения к {host}:{port}")

            # Создаем SFTP-сервис
            sftp_service = SFTPService(
                host=host,
                port=int(port),
                username=username,
                password=password,
            )

            try:
                # Подключаемся к серверу
                sftp_service.connect()
                logger.info(f"🔌 Подключение к серверу {host} установлено")

                # Обрабатываем каждый путь на сервере
                for server_id, path in server_paths:
                    try:
                        # Получаем список файлов
                        files = sftp_service.scan_directory(path)

                        # Если файлы найдены
                        if files:
                            logger.info(f"📦 Обнаружено {len(files)} файлов в {path}")

                            # Отправляем задачи на скачивание
                            for file in files:
                                try:
                                    logger.debug(
                                        f"📄 Отправка задачи для файла: {file.filename}"
                                    )

                                    download_file_task.apply_async(
                                        kwargs={
                                            "host": host,
                                            "port": int(port),
                                            "username": username,
                                            "password": password,
                                            "remote_path": path,
                                            "file": sftp_service.sftp_attr_to_dict(
                                                file
                                            ),
                                            "server_id": server_id,
                                        },
                                        queue="download_queue",
                                    )

                                    processed_files += 1
                                except Exception as e:
                                    logger.error(
                                        f"❌ Ошибка при создании задачи для файла {file.filename}: {str(e)}"
                                    )
                                    total_errors += 1
                        else:
                            logger.info(f"📂 Путь {path} не содержит новых файлов")

                    except Exception as e:
                        logger.error(
                            f"❌ Ошибка при сканировании пути {path} на сервере {host}: {str(e)}"
                        )
                        total_errors += 1

                processed_servers += 1

            except Exception as e:
                logger.error(f"❌ Ошибка при подключении к серверу {host}: {str(e)}")
                total_errors += 1
            finally:
                # Закрываем соединение
                sftp_service.disconnect()
                logger.info(f"🔌 Соединение с сервером {host} закрыто")

        except ValueError as e:
            logger.error(f"❌ Некорректный формат параметров подключения: {str(e)}")
            total_errors += 1
        except Exception as e:
            logger.error(f"❌ Непредвиденная ошибка при обработке сервера: {str(e)}")
            total_errors += 1

    # Формируем отчет
    result = {
        "status": "success" if total_errors == 0 else "partial_success",
        "servers_total": len(union_servers),
        "servers_processed": processed_servers,
        "files_processed": processed_files,
        "errors": total_errors,
    }

    logger.info(
        f"✅ Сканирование завершено: обработано {processed_servers}/{len(union_servers)} серверов, {processed_files} файлов, {total_errors} ошибок"
    )
    return result
