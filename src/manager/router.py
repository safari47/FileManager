from pathlib import Path

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from .crud import FileDAO, ServerDAO
from .dependencies import get_session_with_commit, get_session_without_commit
from .schemas import FileSchema, ServerID, ServerSchema

router = APIRouter()

template = Jinja2Templates(directory=Path(__file__).parent.parent.parent / "templates")


@router.get("/")
def read_root(request: Request):
    """
    Возвращает главную страницу приложения.
    """
    return template.TemplateResponse("index.html", {"request": request})


@router.get(
    "/servers",
    response_model=list[ServerSchema],
    status_code=status.HTTP_200_OK,
    description="Получить список всех серверов",
)
async def get_servers(
    session: AsyncSession = Depends(get_session_without_commit),
    limit: int = Query(default=100, lte=1000),
    offset: int = Query(default=0, ge=0),
):
    return await ServerDAO().find_all(session=session, limit=limit, offset=offset)


@router.post(
    "/servers",
    status_code=status.HTTP_201_CREATED,
    description="Добавить новый сервер для сканирования",
)
async def create_server(
    server: ServerSchema,
    session: AsyncSession = Depends(get_session_with_commit),
):
    """
    Создает новый сервер для сканирования.
    """
    await ServerDAO().add(session=session, values=server)
    return {"message": "Сервер успешно добавлен"}


@router.put(
    "/servers/{server_id}",
    status_code=status.HTTP_200_OK,
    description="Обновить информацию о сервере",
)
async def update_server(
    server_id: int,
    server: ServerSchema,
    session: AsyncSession = Depends(get_session_with_commit),
):
    """
    Обновляет информацию о сервере.
    """
    await ServerDAO().update(
        session=session, filters=ServerID(id=server_id), values=server
    )
    return {"message": "Сервер успешно обновлен"}


@router.delete(
    "/servers/{server_id}", status_code=status.HTTP_200_OK, description="Удалить сервер"
)
async def delete_server(
    server_id: int, session: AsyncSession = Depends(get_session_with_commit)
):
    """
    Удаляет сервер.
    """
    await ServerDAO().delete_by_id(session=session, data_id=server_id)
    return {"message": "Сервер успешно удален"}


@router.get(
    "/files",
    response_model=list[FileSchema],
    status_code=status.HTTP_200_OK,
    description="Получить список всех файлов которые были загружены на сервер",
)
async def get_files(
    session: AsyncSession = Depends(get_session_without_commit),
    limit: int = Query(default=100, lte=1000),
    offset: int = Query(default=0, ge=0),
):
    """
    Получает список всех файлов, которые были загружены на сервер.
    """
    return await FileDAO().find_all(session=session, limit=limit, offset=offset)
