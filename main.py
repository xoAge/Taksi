from fastapi import FastAPI

from app.routers import car_types, clients, drivers, order_statuses, orders, payments, reviews, cars

api = FastAPI(title="Yandex.Taxi", version="1.0.0")
api.include_router(car_types.router)
api.include_router(cars.router)
api.include_router(clients.router)
api.include_router(drivers.router)
api.include_router(order_statuses.router)
api.include_router(orders.router)
api.include_router(payments.router)
api.include_router(reviews.router)


@api.get("/")
def read_root():
    return {"message": "Yandex.Taxi"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(api, host="127.0.0.1", port=8000)
