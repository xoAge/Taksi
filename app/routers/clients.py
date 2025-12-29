from datetime import datetime
from typing import Optional

from fastapi import APIRouter

from app.database import create_entity, get_entities, update_entity, delete_entity
from app.models import Client, Persona, validate_phone, validate_email, validate_past_date

router = APIRouter(prefix="/clients", tags=["clients"])


@router.post("/")
def create_client(name: str, phone: str, email: Optional[str] = None,
                  surname: Optional[str] = None, birthday: Optional[datetime] = None):
    persona = Persona(
        name=name,
        phone=validate_phone(phone, 'phone'),
        registration_date=datetime.now(),
        birthday=birthday
    )
    return create_entity(Client(surname=surname, email=email, persona_rel=persona))


@router.get("/")
def get_clients(client_id: Optional[int] = None):
    return get_entities(Client, client_id)


@router.put("/{client_id}")
def update_client(client_id: int,
                  name: Optional[str] = None,
                  phone: Optional[str] = None,
                  email: Optional[str] = None,
                  surname: Optional[str] = None,
                  birthday: Optional[datetime] = None):
    client_data = {
        "surname": surname,
        "email": validate_email(email, 'email')
    }
    updated_client = update_entity(Client, client_id, client_data)

    persona_data = {
        "name": name,
        "phone": validate_phone(phone, 'phone'),
        "birthday": validate_past_date(birthday, 'birthday')
    }
    update_entity(Persona, updated_client.persona_rel.id, persona_data)

    return updated_client


@router.delete("/{client_id}")
def delete_client(client_id: int):
    delete_entity(Client, client_id)
    delete_entity(Persona, client_id)
    return {"message": "Client deleted"}
