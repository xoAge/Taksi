from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from sqlalchemy import select, func

from app.database import create_entity, session_scope, update_entity
from app.models import Review

router = APIRouter(prefix="/reviews", tags=["reviews"])


@router.post("/")
def create_review(
        author_id: int,
        target_id: int,
        rating: int,
        comment: Optional[str] = None
):
    if rating not in range(1, 5):
        raise HTTPException(
            status_code=400,
            detail=f"Рейтинг должен быть в диапазоне [1..5]"
        )
    return create_entity(Review(
        author_id=author_id,
        target_id=target_id,
        rating=rating,
        comment=comment,
        creation_date=datetime.now()
    ))


@router.get("/")
def get_reviews(
        author_id: Optional[int] = None,
        target_id: Optional[int] = None
):
    with session_scope() as session:
        stmt = select(Review)

        if author_id is not None:
            stmt = stmt.where(Review.author_id == author_id)
        if target_id is not None:
            stmt = stmt.where(Review.target_id == target_id)

        return list(session.scalars(stmt).all())


@router.get("/average/{persona_id}")
def get_average_rating(persona_id: int):
    with session_scope() as session:
        avg = session.scalar(
            select(func.avg(Review.rating)).where(
                Review.target_id == persona_id,
                Review.rating.isnot(None)
            )
        )
        return avg if avg is not None else 5


@router.post("/")
def update_review(
        author_id: int,
        target_id: int,
        rating: int,
        comment: Optional[str] = None
):
    if rating not in range(1, 5):
        raise HTTPException(
            status_code=400,
            detail=f"Рейтинг должен быть в диапазоне [1..5]"
        )
    return update_entity(
        Review,
        {author_id, target_id},
        {
            "rating": rating,
            "comment": comment,
            "creation_date": datetime.now()
        }
    )


@router.delete("/")
def delete_review(
        author_id: int,
        target_id: Optional[int] = None
):
    with session_scope() as session:
        stmt = select(Review).where(Review.author_id == author_id)

        if target_id is not None:
            stmt = stmt.where(Review.target_id == target_id)

        reviews = list(session.scalars(stmt).all())
        for review in reviews:
            session.delete(review)
        session.commit()

        return {"deleted": len(reviews)}
