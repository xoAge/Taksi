from datetime import datetime
from typing import Optional
from fastapi import APIRouter

from app.database import create_entity, get_entities, update_entity
from app.models import Payment, validate_positive

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post("/")
def create_payment(
        order_id: int,
        client_id: int,
        amount: float,
        payment_type: str
):
    return create_entity(Payment(
        order_id=order_id,
        client_id=client_id,
        amount=validate_positive(amount, 'amount'),
        payment_type=payment_type,
        payment_date=None
    ))


@router.post("/{order_id}/pay")
def pay(order_id: int):
    return update_entity(Payment, order_id, {
        "payment_date": datetime.now()
    })


@router.get("/")
def get_payments(order_id: Optional[int] = None):
    return get_entities(Payment, order_id)
