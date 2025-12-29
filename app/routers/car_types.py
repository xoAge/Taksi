from typing import Optional

from fastapi import APIRouter
from app.database import create_entity, get_entities, update_entity, delete_entity
from app.models import CarType

router = APIRouter(prefix="/car-types", tags=["car-types"])


@router.post("/")
def create_car_type(name: str):
    return create_entity(CarType(name=name))


@router.get("/")
def get_car_types(type_id: Optional[int] = None):
    return get_entities(CarType, type_id)


@router.put("/{type_id}")
def rename_car_type(type_id: int, name: str):
    return update_entity(CarType, type_id, {"name": name})


@router.delete("/{type_id}")
def delete_car_type(type_id: int):
    delete_entity(CarType, type_id)
    return {"message": "CarType deleted"}
