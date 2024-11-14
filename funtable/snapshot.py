import os
from datetime import datetime

from funfile.compress import tarfile
from funutil import getLogger

from funtable import DriveTable

logger = getLogger("funtable")


class DriveSnapshot:
    def __init__(self, table: DriveTable, num=7, *args, **kwargs):
        self.num = num
        self.table = table
        self.table.update_partition_dict()

    def delete_outed_version(self):
        files = self.table.partition_meta()
        files = sorted(files, key=lambda f: f["fid"], reverse=True)
        for i, file in enumerate(files):
            if i > self.num:
                logger.info(f"deleted {file['fid']}")
            else:
                logger.info(file)

    @staticmethod
    def _tar_path(file_path):
        return f"{file_path}-{datetime.now().strftime('%Y%m%d%H%M%S')}.tar"

    def update(self, file_path, *args, **kwargs):
        gz_path = self._tar_path(file_path)
        tarfile.file_entar(file_path, gz_path)
        self.table.upload(gz_path, partition=datetime.now().strftime("%Y%m%d%H%M%S"))
        os.remove(gz_path)
        self.delete_outed_version()

    def download(self, dir_path, *args, **kwargs):
        files = self.table.partition_meta()
        files = sorted(files, key=lambda f: f["fid"], reverse=True)

        if len(files) == 0:
            logger.error("没有快照文件")
            return
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        self.table.drive.download_file(fid=files[0]["fid"], local_dir=dir_path)
        tar_path = f"{dir_path}/{files[0]['name']}"
        tarfile.file_detar(tar_path)
        os.remove(tar_path)
