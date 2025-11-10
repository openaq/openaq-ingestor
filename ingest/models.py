from pydantic import BaseModel, Field, ConfigDict, model_validator, AliasChoices
from datetime import datetime
from typing import Optional, Literal, Dict, Any
from decimal import Decimal
from humps import camelize
from enum import Enum
import logging

logger = logging.getLogger(__name__)

# Sub-models for nested structures
class JsonBase(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=camelize,
        protected_namespaces=()
        )

class FetchSummary(JsonBase):
    """Summary statistics from the data fetch"""
    source_name: str = Field(alias="sourceName")
    locations: int = Field(ge=0)
    bounds: list[float] = Field(min_length=4, max_length=4)  # [min_lat, min_lon, max_lat, max_lon]
    systems: int = Field(ge=0)
    sensors: int = Field(ge=0)
    flags: int = Field(ge=0)
    measurements: int = Field(ge=0)
    datetime_from: datetime = Field(alias="datetimeFrom")
    datetime_to: datetime = Field(alias="datetimeTo")
    errors: Dict[str, Any] = Field(default_factory=dict)


class IngestMatchingMethod(str, Enum):
    ingestId = 'ingest-id'
    locationId = 'location-id'
    sensorId = 'sensor-id'

class Meta(JsonBase):
    """Metadata about the data export"""
    ingest_matching_method: IngestMatchingMethod = Field(default='ingest-id', validation_alias=AliasChoices('matching_method', 'ingestMatchingMethod'))
    schema_version: str = Field(
        validation_alias=AliasChoices('schema','schemaVersion')
        )  # Validates version format
    source_name: str = Field(validation_alias=AliasChoices('source', 'sourceName'))
    started_on: Optional[datetime] = None
    finished_on: Optional[datetime] = None
    exported_on: Optional[datetime] = None
    fetch_summary: Optional[FetchSummary] = None


class Coordinates(JsonBase):
    """Geographic coordinates"""
    lat: float = Field(ge=-90, le=90, validation_alias=AliasChoices('latitude'))
    lon: float = Field(ge=-180, le=180, validation_alias=AliasChoices('longitude'))
    proj: str | None = Field(default="WGS84")  # Projection system


class AveragingPeriod(JsonBase):
    """The amount of time that the measurement describes"""
    unit: str
    value: float | int


class LoggingPeriod(JsonBase):
    """The amount of time between logging/averaging events"""
    unit: str
    value: float | int


class Attribution(JsonBase):
    name: Optional[str]
    url: Optional[str]


class Sensor(JsonBase):
    """Sensor configuration and metadata"""
    key: str = Field(min_length=1, alias='sensor_id')
    parameter: str = Field(min_length=1)
    units: str = Field(min_length=1)
    averaging_interval_secs: int = Field(gt=0)
    logging_interval_secs: int = Field(gt=0)
    status: Optional[str] = None
    flags: list[str] = Field(default_factory=list)


class System(JsonBase):
    """Measurement system containing sensors"""
    key: str = Field(min_length=1, alias='system_id')
    manufacturer_name: str = Field(...)
    model_name: str = Field(...)
    sensors: list[Sensor] = Field(min_length=1)


class Location(JsonBase):
    """Physical location with systems"""
    key: str = Field(min_length=1, validation_alias=AliasChoices('key', 'location', 'sensor_node_id'))
    site_id: str = Field(min_length=1, validation_alias=AliasChoices('site_id', 'location', 'key'))
    site_name: str = Field(min_length=1, validation_alias=AliasChoices('site_name', 'site_id','location', 'key'))
    coordinates: Coordinates
    ismobile: bool = Field(default=False)
    systems: Optional[list[System]] = Field(min_length=1, default=[])

    @model_validator(mode='before')
    def preprocess(v):
        if 'lat' in v.keys():
            v['coordinates'] = Coordinates(lat=v.get('lat'), lon=v.get('lon'), proj=v.get('proj'))

        return(v)


class Measure(JsonBase):
    """Individual measurement data point"""
    key: str = Field(min_length=1, validation_alias=AliasChoices('key', 'sensor_id'))
    coordinates: Optional[Coordinates] = None
    timestamp: datetime
    value: float | int  = Field(validation_alias=AliasChoices('measure'))


class DatetimeObject(JsonBase):
    utc: datetime
    local: datetime


class RealtimeMeasure(JsonBase):
    """One row of the ndjson format"""
    date: DatetimeObject
    averaging_period: AveragingPeriod
    location: str
    city: str
    country: str
    coordinates: Coordinates
    attribution: list[Attribution]
    source_name: str
    government: bool = Field(default=False)
    mobile: bool = Field(default=False)


class TransformSchema(JsonBase):
    meta: Meta
    locations: list[Location]
    measures: list[Measure]


class RealtimeSchema(JsonBase):
    measures: list[RealtimeMeasure]


class LcsSchema(TransformSchema):
    @model_validator(mode='before')
    def preprocess(v):
        logger.debug(v)
        return(v)
