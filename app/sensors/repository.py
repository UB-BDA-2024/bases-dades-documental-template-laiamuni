from app import redis_client
from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient
from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional, Dict
from bson.son import SON


from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient) -> Optional[models.Sensor]:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    sensor2 = {
        "id": db_sensor.id,
        "name": sensor.name,
        "latitude": sensor.latitude,
        "longitude": sensor.longitude,
        "type":sensor.type,
        "mac_address":sensor.mac_address,
        "manufacturer":sensor.manufacturer,
        "serie_number":sensor.serie_number,
        "model":sensor.model,
        
        "firmware_version":sensor.firmware_version,
    }
    mongodb.insert(sensor2)

    return db_sensor

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.SensorData:
    db_sensordata = {
        "velocity": data.velocity,
        "temperature": data.temperature,
        "humidity": data.humidity,
        "battery_level": data.battery_level,
        "last_seen": data.last_seen
    }

    redis.set(sensor_id, db_sensordata) 
    return data

def get_data(redis: RedisClient, sensor_id: int, data: Session) -> dict:
    db_sensor = redis.get(sensor_id)

    if db_sensor:
        sensor_data = db_sensor
        sensor_data["id"] = sensor_id
        sensor_data["name"] = data.query(models.Sensor).filter(models.Sensor.id == sensor_id).first().name

        return sensor_data

def delete_sensor(db: Session, sensor_id: int, mongodb: MongoDBClient, redis: RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()

    query = {"id": sensor_id}
    mongodb.delete(query)

    redis.delete(sensor_id)
    return db_sensor

def get_sensor_near(mongodb: MongoDBClient, redis: RedisClient, latitude: float, longitude: float, radius: float, db:Session)->List:
    query = {"latitude":{"$gte":latitude - radius,"$lte":latitude + radius}, "longitude":{"$gte": longitude-radius, "$lte":longitude+radius}}
    documents = list(mongodb.getDocuments(query))

    for i in documents:
        db_sensor = get_sensor(db=db, sensor_id=i['id'])

        db_sensor=get_data(redis=redis,sensor_id=db_sensor.id,data=db)

        i['velocity']=db_sensor['velocity']
        i['temperature']=db_sensor['temperature']
        i['humidity']=db_sensor['humidity']
        i['battery_level']=db_sensor['battery_level']
        i['last_seen']=db_sensor['last_seen']
    
    return documents