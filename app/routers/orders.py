from datetime import datetime
from enum import IntEnum
from typing import Optional

from fastapi import APIRouter, HTTPException

from app.database import create_entity, update_entity, get_entities, session_scope, get_entities_s, update_entity_s
from app.models import Order, Payment, validate_positive, Driver

router = APIRouter(prefix="/orders", tags=["orders"])


class Status(IntEnum):
    CREATED = 1  # Создан
    ASSIGNED = 2  # Прикреплён к водителю
    IN_PROGRESS = 3  # Начат
    FINISHED = 4  # Завершён
    CANCELLED = 5  # Отменён


@router.post("/")
def create_order(
        client_id: int,
        departure_address: str,
        destination_address: str,
        passenger_count: int,
        has_animals: bool,
        has_children: bool,
        has_luggage: bool
):
    return create_entity(Order(
        order_time=datetime.now(),
        departure_address=departure_address,
        destination_address=destination_address,
        passenger_count=validate_positive(passenger_count, 'passenger_count'),
        has_animals=has_animals,
        has_children=has_children,
        has_luggage=has_luggage,
        client_id=client_id,
        status_id=Status.CREATED
    ))


@router.get("/")
def get_orders(order_id: Optional[int] = None):
    return get_entities(Order, order_id)


@router.post("/{order_id}/cancel")
def cancel_order(order_id: int):
    with session_scope() as session:
        status = get_entities_s(session, Order, order_id)
        if status == Status.CREATED or status == Status.ASSIGNED:
            return update_entity_s(session, Order, order_id, {
                "status_id": Status.CANCELLED
            })
        else:
            raise HTTPException(
                status_code=400,
                detail="Можно отменить только необработанные заказы"
            )


@router.post("/{order_id}/assign-driver")
def assign_driver(order_id: int, driver_id: int):
    driver = get_entities(Driver, driver_id)
    if driver is None or not driver.is_working:
        raise HTTPException(
            status_code=400,
            detail="Указанный водитель не существует или не работает"
        )
    return update_entity(Order, order_id, {
        "driver_id": driver_id,
        "status_id": Status.ASSIGNED
    })


@router.post("/{order_id}/start")
def start_trip(order_id: int, amount: float, payment_type: str):
    order = get_entities(Order, order_id)
    if order.status_id != Status.ASSIGNED:
        raise HTTPException(status_code=400, detail="Заказ не привязан к водителю")

    create_entity(Payment(
        order_id=order_id,
        client_id=order.client_id,
        amount=validate_positive(amount, 'amount'),
        payment_type=payment_type,
        payment_date=None
    ))

    return update_entity(Order, order_id, {
        "status_id": Status.IN_PROGRESS
    })


@router.post("/{order_id}/finish")
def finish_trip(order_id: int):
    return update_entity(Order, order_id, {
        "status_id": Status.FINISHED,
        "arrival_time": datetime.now()
    })
