import io
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from funtable.snapshot import DriveSnapshot
from funtable.table import DriveTable


class TestDriveSnapshot(unittest.TestCase):
    def test_drive_table_rebuilds_metadata(self):
        class FakeDrive:
            def __init__(self):
                self.directories = [{"name": "daily", "fid": "daily-fid"}]
                self.files = {"daily-fid": []}
                self.uploads = []

            def get_dir_list(self, fid):
                return self.directories

            def mkdir(self, fid, name):
                directory_fid = f"{name}-fid"
                self.directories.append({"name": name, "fid": directory_fid})
                self.files[directory_fid] = []
                return directory_fid

            def get_file_list(self, fid):
                return self.files[fid]

            def upload_file(self, filepath, fid, overwrite=False):
                self.uploads.append((Path(filepath), fid, overwrite))
                item = {"name": Path(filepath).name, "fid": f"{fid}/file"}
                self.files[fid] = [item]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch("funtable.table.drive.Path.home", return_value=root):
                unsafe_table = DriveTable("../../escape", SimpleNamespace())
                self.assertIn(
                    root.resolve(), Path(unsafe_table._local_meta_path).parents
                )

            payload = root / "payload.txt"
            payload.write_text("snapshot", encoding="utf-8")
            drive = FakeDrive()
            table = DriveTable("root", drive)
            table.__dict__["_local_meta_path"] = str(root / "partition.tar")

            table.update_partition_dict()
            table.upload(payload, "daily", overwrite=True)
            table.update_partition_meta()

            self.assertEqual(table.partition_meta(), drive.files["daily-fid"])
            self.assertEqual(drive.uploads[0], (payload, "daily-fid", True))
            self.assertEqual(drive.uploads[1][1:], ("_meta-fid", True))

    def test_archive_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            source.mkdir()
            (source / "data.txt").write_text("snapshot", encoding="utf-8")
            archive = root / "snapshot.tar.xz"
            destination = root / "restored"
            destination.mkdir()

            DriveSnapshot._archive(source, archive)
            DriveSnapshot._extract(archive, destination)

            self.assertEqual(
                (destination / "source" / "data.txt").read_text(encoding="utf-8"),
                "snapshot",
            )

    def test_unsafe_archive_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "unsafe.tar.xz"
            destination = root / "restored"
            destination.mkdir()
            content = b"unsafe"
            with tarfile.open(archive, "w:xz") as output:
                member = tarfile.TarInfo("../escaped.txt")
                member.size = len(content)
                output.addfile(member, io.BytesIO(content))

            with self.assertRaisesRegex(ValueError, "Unsafe archive member"):
                DriveSnapshot._extract(archive, destination)
            self.assertFalse((root / "escaped.txt").exists())

            snapshot = DriveSnapshot.__new__(DriveSnapshot)
            snapshot.table = SimpleNamespace(
                partition_meta=lambda: [{"name": "../unsafe.tar", "fid": "bad"}]
            )
            snapshot.drive = SimpleNamespace(
                download_file=lambda *args, **kwargs: self.fail("download started")
            )
            with self.assertRaisesRegex(ValueError, "Unsafe snapshot name"):
                snapshot.download(destination)

    def test_download_restores_latest_snapshot_and_cleans_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            source.mkdir()
            (source / "data.txt").write_text("snapshot", encoding="utf-8")
            archive = root / "snapshot-20240102.tar.xz"
            DriveSnapshot._archive(source, archive)

            class FakeDrive:
                def __init__(self, archive_path):
                    self.archive_path = archive_path
                    self.downloads = []

                def download_file(self, fid, destination, overwrite=False):
                    self.downloads.append((fid, destination, overwrite))
                    (Path(destination) / self.archive_path.name).write_bytes(
                        self.archive_path.read_bytes()
                    )

            destination = root / "restored"
            drive = FakeDrive(archive)
            snapshot = DriveSnapshot.__new__(DriveSnapshot)
            snapshot.drive = drive
            snapshot.table = SimpleNamespace(
                partition_meta=lambda: [
                    {"name": archive.name, "fid": "latest"},
                    {"name": "snapshot-20240101.tar.xz", "fid": "older"},
                ]
            )

            snapshot.download(destination)

            self.assertEqual(drive.downloads, [("latest", str(destination), True)])
            self.assertEqual(
                (destination / "source" / "data.txt").read_text(encoding="utf-8"),
                "snapshot",
            )
            self.assertFalse((destination / archive.name).exists())

    def test_update_cleans_archive_and_keeps_exact_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "data.txt"
            source.write_text("snapshot", encoding="utf-8")
            state = {"metadata_updated": False, "uploaded": None}
            files = [
                {"name": "data-20240103.tar.xz", "fid": "new"},
                {"name": "data-20240102.tar.xz", "fid": "middle"},
                {"name": "data-20240101.tar.xz", "fid": "old"},
            ]

            def upload(path, partition, overwrite):
                self.assertTrue(Path(path).exists())
                self.assertEqual(partition, "daily")
                self.assertTrue(overwrite)
                state["uploaded"] = Path(path)

            def update_partition_meta():
                state["metadata_updated"] = True

            deleted = []
            snapshot = DriveSnapshot.__new__(DriveSnapshot)
            snapshot.num = 2
            snapshot.drive = SimpleNamespace(delete=deleted.append)
            snapshot.table = SimpleNamespace(
                upload=upload,
                update_partition_meta=update_partition_meta,
                partition_meta=lambda: files,
            )

            snapshot.update(source, partition="daily")

            self.assertTrue(state["metadata_updated"])
            self.assertFalse(state["uploaded"].exists())
            self.assertEqual(deleted, ["old"])


if __name__ == "__main__":
    unittest.main()
