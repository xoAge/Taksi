from datetime import datetime
from typing import Optional

from fastapi import APIRouter

from app.database import create_entity, get_entities, update_entity, delete_entity
from app.models import Driver, Persona, validate_phone, validate_past_date

router = APIRouter(prefix="/drivers", tags=["drivers"])


@router.post("/")
def create_driver(name: str, phone: str, surname: str,
                  license_number: str, car_id: int):
    persona = Persona(
        name=name,
        phone=validate_phone(phone, 'phone'),
        registration_date=datetime.now()
    )
    return create_entity(Driver(
        surname=surname,
        license_number=license_number,
        is_working=True,
        car_id=car_id,
        persona_rel=persona
    ))


@router.get("/")
def get_drivers(driver_id: Optional[int] = None):
    return get_entities(Driver, driver_id)


@router.put("/{driver_id}")
def update_driver(driver_id: int,
                  name: Optional[str] = None,
                  phone: Optional[str] = None,
                  surname: Optional[str] = None,
                  license_number: Optional[str] = None,
                  is_working: Optional[bool] = None,
                  car_id: Optional[int] = None,
                  birthday: Optional[datetime] = None):
    driver_data = {
        "surname": surname,
        "license_number": license_number,
        "is_working": is_working,
        "car_id": car_id
    }
    updated_driver = update_entity(Driver, driver_id, driver_data)

    persona_data = {
        "name": name,
        "phone": validate_phone(phone, 'phone'),
        "birthday": validate_past_date(birthday, 'birthday')
    }
    update_entity(Persona, updated_driver.persona_rel.id, persona_data)

    return updated_driver


@router.delete("/{driver_id}")
def delete_driver(driver_id: int):
    delete_entity(Driver, driver_id)
    delete_entity(Persona, driver_id)
    return {"message": "Driver deleted"}
