from datetime import datetime
from typing import Optional

from fastapi import APIRouter
from app.database import create_entity, get_entities, update_entity, delete_entity
from app.models import Car

router = APIRouter(prefix="/cars", tags=["cars"])


@router.post("/")
def create_car(brand: str, model: str, license_plate: str, color: str,
               year: str, is_personal: bool, car_type_id: int):
    return create_entity(Car(
        brand=brand, model=model, license_plate=license_plate, color=color, 
        year=year, is_personal=is_personal, car_type_id=car_type_id)
    )


@router.get("/")
def get_cars(car_id: Optional[int] = None):
    return get_entities(Car, car_id)


@router.put("/{car_id}")
def update_car(
    car_id: int,
    brand: Optional[str] = None,
    model: Optional[str] = None,
    license_plate: Optional[str] = None,
    color: Optional[str] = None,
    year: Optional[str] = None,
    is_personal: Optional[bool] = None,
    car_type_id: Optional[int] = None
):
    update_entity(Car, car_id, {
        "brand": brand,
        "model": model,
        "license_plate": license_plate,
        "color": color,
        "year": year,
        "is_personal": is_personal,
        "car_type_id": car_type_id
    })


@router.delete("/{car_id}")
def delete_car(car_id: int):
    delete_entity(Car, car_id)
    return {"message": "Car deleted"}
