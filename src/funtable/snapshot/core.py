from __future__ import annotations

import sys
import tarfile
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import TYPE_CHECKING

from ..table import DriveTable

if TYPE_CHECKING:
    from fundrive.core import BaseDrive

logger = getLogger("funtable")


class DriveSnapshot:
    def __init__(self, table_fid, drive: BaseDrive, num=7):
        if num < 1:
            raise ValueError("num must be at least 1")
        self.num = num
        self.drive = drive
        self.table = DriveTable(table_fid=table_fid, drive=drive)
        self.table.update_partition_dict()

    def delete_outed_version(self):
        files = sorted(
            self.table.partition_meta(), key=lambda file: file["name"], reverse=True
        )
        for file in files[self.num :]:
            logger.info(f"deleted {file['fid']}")
            self.drive.delete(file["fid"])

    @staticmethod
    def _tar_path(file_path):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{file_path}-{timestamp}.tar.xz"

    @staticmethod
    def _archive(file_path, archive_path):
        source = Path(file_path)
        with tarfile.open(archive_path, "w:xz") as archive:
            archive.add(source, arcname=source.name)

    @staticmethod
    def _extract(archive_path, dir_path):
        destination = Path(dir_path).resolve()
        with tarfile.open(archive_path, "r:*") as archive:
            for member in archive.getmembers():
                target = (destination / member.name).resolve()
                outside_destination = (
                    destination != target and destination not in target.parents
                )
                if (
                    outside_destination
                    or member.issym()
                    or member.islnk()
                    or member.isdev()
                ):
                    raise ValueError(f"Unsafe archive member: {member.name}")
            if sys.version_info >= (3, 12):
                archive.extractall(destination, filter="data")
            else:
                archive.extractall(destination)

    def update(self, file_path, partition=None):
        archive_path = Path(self._tar_path(file_path))
        try:
            self._archive(file_path, archive_path)
            self.table.upload(
                archive_path,
                partition=partition or datetime.now().strftime("%Y%m%d%H%M%S"),
                overwrite=True,
            )
        finally:
            archive_path.unlink(missing_ok=True)
        self.table.update_partition_meta()
        self.delete_outed_version()

    def download(self, dir_path):
        files = sorted(
            self.table.partition_meta(), key=lambda file: file["name"], reverse=True
        )
        if not files:
            logger.error("没有快照文件")
            return

        destination = Path(dir_path)
        destination.mkdir(parents=True, exist_ok=True)
        archive_name = Path(files[0]["name"]).name
        if archive_name != files[0]["name"]:
            raise ValueError(f"Unsafe snapshot name: {files[0]['name']}")
        archive_path = destination / archive_name
        try:
            self.drive.download_file(files[0]["fid"], str(destination), overwrite=True)
            self._extract(archive_path, destination)
        finally:
            archive_path.unlink(missing_ok=True)
