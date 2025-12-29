from typing import Optional

from fastapi import APIRouter

from app.database import create_entity, get_entities, update_entity, delete_entity
from app.models import OrderStatus

router = APIRouter(prefix="/order-statuses", tags=["order-statuses"])


@router.post("/")
def create_order_status(value: str):
    return create_entity(OrderStatus(value=value))


@router.get("/")
def get_order_statuses(status_id: Optional[int] = None):
    return get_entities(OrderStatus, status_id)


@router.put("/{status_id}")
def rename_order_status(status_id: int, value: str):
    return update_entity(OrderStatus, status_id, {"value": value})


@router.delete("/{status_id}")
def delete_order_status(status_id: int):
    delete_entity(OrderStatus, status_id)
    return {"message": "OrderStatus deleted"}
