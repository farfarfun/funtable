from abc import abstractmethod
from datetime import datetime, timezone
from hashlib import md5
from typing import Any, TypeVar

from farlog import getLogger
from sqlmodel import Field, Session, SQLModel, select

logger = getLogger("funtable")

T = TypeVar("T", bound="BaseModel")


def _utc_now() -> datetime:
    """返回带 UTC 时区信息的当前时间。"""
    return datetime.now(timezone.utc)


class BaseModel(SQLModel):
    """提供常用查询、写入和唯一标识能力的 SQLModel 基类。"""

    id: int | None = Field(description="自增ID", default=None, primary_key=True)
    uid: str | None = Field(description="唯一ID", default="", unique=True)
    gmt_create: datetime | None = Field(
        description="创建时间", default_factory=_utc_now
    )
    gmt_modified: datetime | None = Field(
        description="修改时间",
        default_factory=_utc_now,
        sa_column_kwargs={"onupdate": _utc_now},
    )

    @classmethod
    def by_id(cls: type[T], _id: int, session: Session) -> T | None:
        """按自增 ID 查询记录，不存在时返回 ``None``。"""
        obj = session.get(cls, _id)
        if obj is None:
            logger.error(f"{cls.__name__} with id {_id} not found")
        return obj

    @classmethod
    def by_uid(cls: type[T], uid: str, session: Session) -> T | None:
        """按唯一 ID 查询记录，不存在时返回 ``None``。"""
        obj = session.exec(select(cls).where(cls.uid == uid)).first()
        if obj is None:
            logger.error(f"{cls.__name__} with uid = {uid} not found")
        return obj

    @classmethod
    def all(cls: type[T], session: Session) -> list[T]:
        """返回当前模型的全部记录。"""
        return list(session.exec(select(cls)).all())

    @classmethod
    def __transform(cls: type[T], source: object) -> T | None:
        if isinstance(source, (dict, SQLModel)):
            obj = cls.model_validate(source)
        else:
            return None
        obj.__set_unique()
        return obj

    @classmethod
    def create(
        cls: type[T], source: dict[str, Any] | SQLModel, session: Session
    ) -> T | None:
        """创建、提交并刷新一条记录。"""
        obj = cls.__transform(source)
        if obj is None:
            return None
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj

    @classmethod
    def upsert(
        cls: type[T],
        source: dict[str, Any] | SQLModel,
        session: Session,
        commit: bool = False,
    ) -> T | None:
        """按唯一 ID 新增或更新记录，可选择立即提交。"""
        obj = cls.__transform(source)
        if obj is None:
            return None
        assert obj.uid is not None
        result = cls.by_uid(obj.uid, session)
        if result is None:
            result = obj
        else:
            for key, value in obj.model_dump(
                exclude_unset=True, exclude={"id", "gmt_create", "uid"}
            ).items():
                setattr(result, key, value)
        if commit:
            session.add(result)
            session.commit()
            session.refresh(result)

        return result

    def update(self: T, source: dict[str, Any] | SQLModel, session: Session) -> T:
        """用传入字段更新当前记录并提交。"""
        obj = self.__transform(source)
        if obj is None:
            return self
        for key, value in obj.model_dump(
            exclude_unset=True, exclude={"id", "gmt_create", "uid"}
        ).items():
            setattr(self, key, value)

        session.merge(self)
        session.commit()
        session.refresh(self)
        return self

    def delete(self: T, session: Session) -> T:
        """删除记录"""
        session.delete(self)
        session.commit()
        return self

    def __set_unique(self) -> None:
        self.uid = md5(
            self.unique_str().encode("utf-8"), usedforsecurity=False
        ).hexdigest()

    @abstractmethod
    def unique_str(self) -> str:
        """返回用于生成唯一 ID 的稳定字符串。"""
        raise NotImplementedError
