import os, sys
import reprlib
import time
from queue import Queue

from dask import bag
from dask.delayed import delayed
from functools import wraps
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil
import subprocess

from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler, visualize
from distributed import Client

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()
from django.conf import settings

from capdb.storages import ingest_storage, zip_storage
from scripts.ingest_by_manifest import get_inventory_files_from_manifest, get_latest_manifest, read_inventory_file, \
    trim_csv_key
from scripts.helpers import copy_file

from joblib import Memory
memory = Memory(cachedir=os.path.join(settings.BASE_DIR, '../.cache'), verbose=0)

MAX_INVENTORY_FILES = 10
MAX_FILES_PER_INVENTORY = 10

def log(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(func, reprlib.repr(args), repr(kwargs))
        return func(*args, **kwargs)
    return wrapper

### init ###

@log
# @memory.cache
def get_volumes():
    volumes = ingest_storage.iter_files("")
    current_vol = next(volumes, "")
    while current_vol:
        next_vol = next(volumes, "")
        if current_vol.split("_", 1)[0] != next_vol.split("_", 1)[0]:
            yield current_vol
        current_vol = next_vol
    # # build set of volumes
    # all_volumes = sorted(ingest_storage.iter_files(""))
    # latest_volumes = set({volume.split("_", 1)[0]: volume for volume in all_volumes}.values())
    # blacklist = set(all_volumes) - latest_volumes
    # return latest_volumes, blacklist

@log
def get_zip_files():
    # build set of existing zip files
    return set(zip_storage.iter_files(""))

@log
def get_unzipped_volumes():
    zip_files = get_zip_files()
    for volume in get_volumes():
        if volume + ".zip" not in zip_files:
            yield volume

# ### get inventory files ###
#
# @log
# def get_inventory_files():
#     for i, inventory_path in enumerate(get_inventory_files_from_manifest(get_latest_manifest())):
#         yield inventory_path
#         if i >= MAX_INVENTORY_FILES:
#             return
#
# ### get ingest files ###
#
# @log
# def get_volume_files(inventory_file, blacklist):
#     for i, inventory_item in enumerate(read_inventory_file(inventory_file)):
#         key = inventory_item[1]
#         if not key.startswith(settings.INVENTORY['csv_path_prefix']):
#             continue
#         key = trim_csv_key(key)
#         volume = key.split("/", 1)[0]
#         if volume in blacklist:
#             continue
#         yield (volume, [key])
#
#         if MAX_FILES_PER_INVENTORY and i >= MAX_FILES_PER_INVENTORY:
#             return
#
# @log
# def volume_fold_binop(totals, volume):
#     return totals + volume[1]
#
# @log
# def volume_fold_combine(a, b):
#     return a + b

@log
def get_volume_files(volume_path):
    for path in ingest_storage.iter_files_recursive(volume_path):
        yield path

### compress volumes ###

@log
def compress_volume(volume_name):
    volume_name
    volume_files = get_volume_files(volume_name)

    jp2_count = 0

    # make a copy of volume in a temp dir
    with TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)
        volume_path = tempdir / volume_name
        volume_path.mkdir()
        for volume_file_path in volume_files:
            out_path = tempdir / volume_file_path
            out_path.parents[0].mkdir(parents=True, exist_ok=True)

            # compress jp2s to jpg
            if volume_file_path.endswith('.jp2'):
                jp2_count += 1
                if jp2_count > 10:
                    continue
                print("Compressing", volume_file_path)
                start_time = time.time()
                with ingest_storage.open(volume_file_path, "rb") as im_file:
                    # note: tried pillow here instead of imagemagick, ala https://www.reddit.com/r/Python/comments/4b6bew/using_pilpillow_with_mozjpeg/
                    # but found that it added a red tint to parsed jp2s
                    jpg_process = subprocess.Popen(
                        'convert JP2:- TGA:- | /usr/local/opt/mozjpeg/bin/cjpeg -quality 40 -targa',
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        shell=True
                    )
                    output_stdout, output_stderr = jpg_process.communicate(im_file.read())
                    if output_stderr:
                        raise Exception("Error encoding jpeg: %s" % output_stderr)
                    with out_path.with_suffix('.jpg').open("wb") as out:
                        out.write(output_stdout)
                print("Compress time:", time.time()-start_time)

            # copy regular files
            else:
                copy_file(volume_file_path, str(out_path), from_storage=ingest_storage)

        # zip volume file
        zip_path = tempdir / volume_name
        zip_path = Path(shutil.make_archive(str(zip_path), "zip", str(volume_path)))

        # write zip to storage
        with zip_path.open("rb") as in_file:
            with zip_storage.open(zip_name, "wb") as out_file:
                shutil.copyfileobj(in_file, out_file)


def run_pipeline():
    # setup -- we use these lists of existing volumes and zip files to make processing more efficient later
    # zip_files = get_zip_files()
    # latest_volumes, blacklist = get_volumes()
    # num_workers = os.cpu_count()  # default used by dask.multiprocessing.get
    #
    # # series of delayed steps:
    #
    # # load each inventory file in parallel
    # volume_bag = bag.from_delayed(delayed(get_volume_files)(inventory_file, blacklist) for inventory_file in get_inventory_files())
    #
    # # group volume files by volume
    # volume_bag = volume_bag.foldby(0, volume_fold_binop, [], volume_fold_combine, [])
    #
    # # foldby gathers everything onto one worker -- once that's done, we have to redistribute the work back out
    # volume_bag = volume_bag.repartition(num_workers)
    #
    # # zip each volume
    # volume_bag = volume_bag.map(compress_volume, zip_files)
    #
    # # visualize tree
    # from dask.dot import dot_graph
    # dot_graph(volume_bag.dask)
    #
    # # run all steps
    # with Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
    #     volume_bag.compute()
    #     visualize([prof, rprof, cprof])

    client = Client()
    input_q = Queue()
    remote_q = client.scatter(input_q)
    zipped_q = client.map(compress_volume, remote_q)
    result_q = client.gather(zipped_q)
    for volume_name in get_unzipped_volumes():
        input_q.put(volume_name)

if __name__ == "__main__":
    run_pipeline()


# next: https://github.com/ogrisel/docker-distributed