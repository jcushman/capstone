import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()
from django.conf import settings

import itertools
from functools import wraps
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

from capdb.storages import ingest_storage, zip_storage
from scripts.ingest_by_manifest import get_inventory_files_from_manifest, get_latest_manifest, read_inventory_file, \
    trim_csv_key


def log(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(func, args, kwargs)
        return func(*args, **kwargs)
    return wrapper

class MRCompressVolumes(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_init=self.init,
                mapper=self.mapper_get_inventory_files
            ),
            MRStep(
                mapper=self.mapper_get_volume_files,
                combiner=self.reducer_volume_files,
                reducer=self.reducer_volume_files,
            ),
            MRStep(
                mapper=self.mapper_compress_volume,
            ),
        ]

    ### init ###

    @log
    def init(self):
        # build set of volumes
        volumes = sorted(ingest_storage.iter_files(""))
        volumes = {volume.split("_", 1)[0]: volume for volume in volumes}.values()
        self.volumes = set(volumes)

        # build set of existing zip files
        self.zip_files = set(zip_storage.iter_files(""))

    ### get inventory files ###

    @log
    def mapper_get_inventory_files(self, k, v):
        for inventory_path in get_inventory_files_from_manifest(get_latest_manifest()):
            yield (inventory_path, 1)

    ### get ingest files ###

    @log
    def mapper_get_volume_files(self, inventory_file, _):
        for inventory_item in read_inventory_file(inventory_file):
            key = inventory_item[1]
            if not key.startswith(settings.INVENTORY['csv_path_prefix']):
                continue
            key = trim_csv_key(key)
            volume = key.split("/", 1)[0]
            if not volume in self.volumes:
                continue
            yield (volume, [key])

    @log
    def reducer_volume_files(self, volume, files):
        yield (volume, list(itertools.chain(*files)))

    ### compress volumes ###

    @log
    def mapper_compress_volume(self, volume_name, volume_files):
        # skip existing zips
        zip_name = volume_name+".zip"
        if zip_name in self.zip_files:
            return

        # make a copy of volume in a temp dir
        with TemporaryDirectory() as tempdir:
            tempdir = Path(tempdir)
            volume_path = tempdir / volume_name
            volume_path.mkdir()
            for volume_file in volume_files:
                out_path = tempdir / volume_file
                out_path.parents[0].mkdir(parents=True, exist_ok=True)
                with ingest_storage.open(volume_file, "rb") as in_file:
                    with out_path.open("wb") as out_file:
                        shutil.copyfileobj(in_file, out_file)

            # zip volume file
            zip_path = tempdir / volume_name
            zip_path = Path(shutil.make_archive(str(zip_path), "zip", str(volume_path)))

            # write zip to storage
            with zip_path.open("rb") as in_file:
                with zip_storage.open(zip_name, "wb") as out_file:
                    shutil.copyfileobj(in_file, out_file)

if __name__ == '__main__':
    MRCompressVolumes.run()