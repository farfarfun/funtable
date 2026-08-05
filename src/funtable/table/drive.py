from __future__ import annotations

import json
from functools import cached_property
from hashlib import sha256
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fundrive.core import BaseDrive

logger = getLogger("funtable")


class DriveTable:
    def __init__(self, table_fid, drive: BaseDrive):
        self.table_fid = table_fid
        self.drive = drive
        self._fid_par_dict = {}
        self._fid_meta = None
        self._fid_meta_par = None

    @cached_property
    def _local_meta_path(self):
        cache_key = sha256(str(self.table_fid).encode()).hexdigest()
        cache_dir = Path.home() / ".cache" / "fundrive" / "table" / cache_key
        cache_dir.mkdir(parents=True, exist_ok=True)
        return str(cache_dir / "partition.tar")

    @property
    def meta_path(self):
        if not self._fid_par_dict:
            self.update_partition_dict()
        return self._fid_meta

    def update_partition_dict(self):
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

    def upload(self, file, partition, overwrite=False):
        fid = self._fid_par_dict.get(partition)
        if fid is None:
            logger.info(f"partition={partition} not exists, create it")
            fid = self.drive.mkdir(fid=self.table_fid, name=partition)
            self._fid_par_dict[partition] = fid
        logger.info(f"upload file={file} partition={partition} fid={fid}")
        self.drive.upload_file(file, fid, overwrite=overwrite)

    def update_partition_meta(self):
        if self._fid_meta is None:
            self.update_partition_dict()

        partition_meta = {}
        for partition_name, partition_fid in self._fid_par_dict.items():
            if partition_name.startswith("_"):
                continue
            for file in self.drive.get_file_list(partition_fid):
                partition_meta[file["name"]] = file

        with open(self._local_meta_path, "w", encoding="utf-8") as file:
            json.dump(list(partition_meta.values()), file)
        self.drive.upload_file(self._local_meta_path, self._fid_meta, overwrite=True)
        self.update_partition_dict()

    def partition_meta(self, refresh=False):
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
            return json.load(file)
