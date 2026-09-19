from __future__ import annotations

import json
from functools import cached_property
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any

from farlog import getLogger

if TYPE_CHECKING:
    from fundrive.core import BaseDrive

logger = getLogger("funtable")


class DriveTable:
    """管理云盘中的分区目录和元数据。"""

    def __init__(self, table_fid: str, drive: BaseDrive) -> None:
        """使用云盘表目录 ID 和驱动实例初始化。"""
        self.table_fid = table_fid
        self.drive = drive
        self._fid_par_dict: dict[str, str] = {}
        self._fid_meta: str | None = None
        self._fid_meta_par: str | None = None

    @cached_property
    def _local_meta_path(self) -> str:
        cache_key = sha256(str(self.table_fid).encode()).hexdigest()
        cache_dir = Path.home() / ".cache" / "fundrive" / "table" / cache_key
        cache_dir.mkdir(parents=True, exist_ok=True)
        return str(cache_dir / "partition.tar")

    @property
    def meta_path(self) -> str | None:
        """返回云盘元数据目录 ID，必要时先刷新分区。"""
        if not self._fid_par_dict:
            self.update_partition_dict()
        return self._fid_meta

    def update_partition_dict(self) -> None:
        """从云盘刷新分区名称与目录 ID 的映射。"""
        self._fid_par_dict = {
            file["name"]: file["fid"]
            for file in self.drive.get_dir_list(self.table_fid)
        }
        self._fid_meta = self._fid_par_dict.get("_meta")
        if self._fid_meta is None:
            self._fid_meta = self.drive.mkdir(fid=self.table_fid, name="_meta")
            self._fid_par_dict["_meta"] = self._fid_meta

        self._fid_meta_par = next(
            (
                file["fid"]
                for file in self.drive.get_file_list(self._fid_meta)
                if file["name"] == Path(self._local_meta_path).name
            ),
            None,
        )
        logger.info(f"partition_size={len(self._fid_par_dict)}")

    def upload(self, file: str | Path, partition: str, overwrite: bool = False) -> None:
        """把文件上传到指定分区，不存在时自动创建分区。"""
        fid = self._fid_par_dict.get(partition)
        if fid is None:
            logger.info(f"partition={partition} not exists, create it")
            fid = self.drive.mkdir(fid=self.table_fid, name=partition)
            self._fid_par_dict[partition] = fid
        logger.info(f"upload file={file} partition={partition} fid={fid}")
        self.drive.upload_file(file, fid, overwrite=overwrite)

    def update_partition_meta(self) -> None:
        """汇总所有分区文件并上传最新元数据。"""
        if self._fid_meta is None:
            self.update_partition_dict()

        partition_meta: dict[str, dict[str, Any]] = {}
        for partition_name, partition_fid in self._fid_par_dict.items():
            if partition_name.startswith("_"):
                continue
            for file in self.drive.get_file_list(partition_fid):
                partition_meta[file["name"]] = file

        with open(self._local_meta_path, "w", encoding="utf-8") as file:
            json.dump(list(partition_meta.values()), file)
        self.drive.upload_file(self._local_meta_path, self._fid_meta, overwrite=True)
        self.update_partition_dict()

    def partition_meta(self, refresh: bool = False) -> list[dict[str, Any]]:
        """读取分区元数据；refresh 为真时强制重新下载。"""
        meta_path = Path(self._local_meta_path)
        if refresh:
            meta_path.unlink(missing_ok=True)
        if not meta_path.exists() and self._fid_meta_par is not None:
            self.drive.download_file(
                self._fid_meta_par, str(meta_path.parent), overwrite=True
            )
        if not meta_path.exists():
            return []
        with meta_path.open(encoding="utf-8") as file:
            data: list[dict[str, Any]] = json.load(file)
            return data
