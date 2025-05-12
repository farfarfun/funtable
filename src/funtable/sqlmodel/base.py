from datetime import datetime
from typing import List, Optional, TypeVar, Union

from funutil import getLogger
from sqlmodel import Field, Session, SQLModel, select

logger = getLogger("funtable")

T = TypeVar("T", bound="BaseModel")


class BaseModel(SQLModel):
    id: Optional[int] = Field(description="自增ID", default=None, primary_key=True)
    uid: Optional[str] = Field(description="唯一ID", default="", unique=True)
    gmt_create: Optional[datetime] = Field(
        description="创建时间", default_factory=datetime.now
    )
    gmt_modified: Optional[datetime] = Field(
        description="修改时间",
        default_factory=datetime.now,
        sa_column_kwargs={"onupdate": datetime.now},
    )

    @classmethod
    def by_id(cls, _id: int, session) -> T:
        obj = session.get(cls, _id)
        if obj is None:
            logger.error(f"{cls.__name__} with id {_id} not found")
        return obj

    @classmethod
    def all(cls, session) -> List[T]:
        return session.exec(select(cls)).all()

    @classmethod
    def create(cls, source: Union[dict, SQLModel], session: Session) -> T:
        if isinstance(source, SQLModel):
            obj = cls.model_validate(source)
        elif isinstance(source, dict):
            obj = cls.model_validate(source)
        else:
            return None
        obj.parse()
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj

    @classmethod
    def upsert(cls, source: Union[dict, SQLModel], session: Session) -> T:
        if isinstance(source, SQLModel):
            obj = cls.model_validate(source)
        elif isinstance(source, dict):
            obj = cls.model_validate(source)
        else:
            return None
        obj.parse()
        statement = select(cls).where(cls.uid == obj.uid)
        result = session.exec(statement).first()

        if result is None:
            result = obj
        else:
            for key, value in obj.model_dump(
                exclude_unset=True, exclude={"id", "gmt_create", "uid"}
            ).items():
                setattr(result, key, value)

        session.add(result)
        session.commit()
        session.refresh(result)

        return result

    def update(self, source: Union[dict, SQLModel], session: Session) -> T:
        if isinstance(source, SQLModel):
            obj = self.model_validate(source)
        elif isinstance(source, dict):
            obj = self.model_validate(source)
        else:
            return None
        obj.parse()

        for key, value in obj.model_dump(
            exclude_unset=True, exclude={"id", "gmt_create", "uid"}
        ).items():
            setattr(self, key, value)

        session.merge(self)
        session.commit()
        session.refresh(self)
        return self

    def parse(self):
        pass
