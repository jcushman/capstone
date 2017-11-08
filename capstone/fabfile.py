import csv
import glob
import gzip
import hashlib
import os
from datetime import datetime
import django
import zipfile

# set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
try:
    django.setup()
except Exception as e:
    print("WARNING: Can't configure Django -- tasks depending on Django will fail:\n%s" % e)

from django.conf import settings
from django.contrib.auth.models import User
from fabric.api import local
from fabric.decorators import task

from capapi import resources
from capdb.models import Jurisdiction, CaseMetadata
# from process_ingested_xml import fill_case_page_join_table
from scripts import set_up_postgres, ingest_tt_data, ingest_files, data_migrations, ingest_by_manifest


@task(alias='run')
def run_django():
    local("python manage.py runserver")

@task
def test():
    """ Run tests with coverage report. """
    local("pytest --fail-on-template-vars --cov --cov-report=")

@task
def recent_sync_with_s3():
    ingest_by_manifest.sync_recent_data()

@task
def total_sync_with_s3():
    ingest_by_manifest.complete_data_sync()

@task
def ingest_volumes():
    ingest_files.ingest_volumes()

@task
def update_case_metadata():
    ingest_files.update_case_metadata()

@task
def ingest_jurisdiction():
    ingest_tt_data.populate_jurisdiction()

@task
def ingest_metadata():
    ingest_tt_data.populate_jurisdiction()
    ingest_tt_data.ingest(False)

@task
def sync_metadata():
    ingest_tt_data.ingest(True)

@task
def run_pending_migrations():
    data_migrations.run_pending_migrations()

@task
def update_postgres_env():
    set_up_postgres.update_postgres_env()

@task
def init_db():
    """
        Set up new dev database.
    """
    migrate()

    print("Creating DEV admin user:")
    User.objects.create_superuser('admin', 'admin@example.com', 'admin')

    update_postgres_env()

@task
def migrate():
    """
        Migrate all dbs at once
    """
    local("python manage.py migrate --database=default")
    local("python manage.py migrate --database=capapi")
    if settings.USE_TEST_TRACKING_TOOL_DB:
        local("python manage.py migrate --database=tracking_tool")

@task
def load_test_data():
    if settings.USE_TEST_TRACKING_TOOL_DB:
        local("python manage.py loaddata --database=tracking_tool test_data/tracking_tool.json")
    ingest_metadata()
    ingest_jurisdiction()
    total_sync_with_s3()

@task
def add_test_case(*barcodes):
    """
        Write test data and fixtures for given volume and case. Example: fab add_test_case:32044057891608_0001

        NOTE:
            DATABASES['tracking_tool'] must point to real tracking tool db.
            STORAGES['ingest_storage'] must point to real harvard-ftl-shared.

        Output is stored in test_data/tracking_tool.json and test_data/from_vendor.
        Tracking tool user details are anonymized.
    """

    from django.core import serializers
    from tracking_tool.models import Volumes, Reporters, BookRequests, Pstep, Eventloggers, Hollis, Users
    from capdb.storages import ingest_storage
    from scripts.helpers import parse_xml, copy_file, resolve_namespace

    ## write S3 files to local disk

    for barcode in barcodes:

        print("Writing data for", barcode)

        volume_barcode, case_number = barcode.split('_')

        # get volume dir
        source_volume_dirs = list(ingest_storage.iter_files(volume_barcode, partial_path=True))
        if not source_volume_dirs:
            print("ERROR: Can't find volume %s. Skipping!" % volume_barcode)
        source_volume_dir = sorted(source_volume_dirs, reverse=True)[0]

        # make local dir
        dest_volume_dir = os.path.join(settings.BASE_DIR, 'test_data/from_vendor/%s' % os.path.basename(source_volume_dir))
        os.makedirs(dest_volume_dir, exist_ok=True)

        # copy volume-level files
        for source_volume_path in ingest_storage.iter_files(source_volume_dir):
            dest_volume_path = os.path.join(dest_volume_dir, os.path.basename(source_volume_path))
            if '.' in os.path.basename(source_volume_path):
                # files
                copy_file(source_volume_path, dest_volume_path, from_storage=ingest_storage)
            else:
                # dirs
                os.makedirs(dest_volume_path, exist_ok=True)

        # read volmets xml
        source_volmets_path = glob.glob(os.path.join(dest_volume_dir, '*.xml'))[0]
        with open(source_volmets_path) as volmets_file:
            volmets_xml = parse_xml(volmets_file.read())

        # copy case file and read xml
        source_case_path = volmets_xml.find('mets|file[ID="casemets_%s"] > mets|FLocat' % case_number).attr(resolve_namespace('xlink|href'))
        source_case_path = os.path.join(source_volume_dir, source_case_path)
        dest_case_path = os.path.join(dest_volume_dir, source_case_path[len(source_volume_dir)+1:])
        copy_file(source_case_path, dest_case_path, from_storage=ingest_storage)
        with open(dest_case_path) as case_file:
            case_xml = parse_xml(case_file.read())

        # copy support files for case
        for flocat_el in case_xml.find('mets|FLocat'):
            source_path = os.path.normpath(os.path.join(os.path.dirname(source_case_path), flocat_el.attrib[resolve_namespace('xlink|href')]))
            dest_path = os.path.join(dest_volume_dir, source_path[len(source_volume_dir) + 1:])
            copy_file(source_path, dest_path, from_storage=ingest_storage)

        # remove unused files from volmets
        local_files = glob.glob(os.path.join(dest_volume_dir, '*/*'))
        local_files = [x[len(dest_volume_dir)+1:] for x in local_files]
        for flocat_el in volmets_xml.find('mets|FLocat'):
            if not flocat_el.attrib[resolve_namespace('xlink|href')] in local_files:
                file_el = flocat_el.getparent()
                file_el.getparent().remove(file_el)
        with open(source_volmets_path, "w") as out_file:
            out_file.write(str(volmets_xml))

    ## load metadata into JSON fixtures from tracking tool

    to_serialize = set()
    user_ids = set()
    volume_barcodes = [os.path.basename(d).split('_')[0] for d in
                glob.glob(os.path.join(settings.BASE_DIR, 'test_data/from_vendor/*'))]

    for volume_barcode in volume_barcodes:

        print("Updating metadata for", volume_barcode)

        tt_volume = Volumes.objects.get(bar_code=volume_barcode)
        to_serialize.add(tt_volume)

        user_ids.add(tt_volume.created_by)

        tt_reporter = Reporters.objects.get(id=tt_volume.reporter_id)
        to_serialize.add(tt_reporter)

        to_serialize.update(Hollis.objects.filter(reporter_id=tt_reporter.id))

        request = BookRequests.objects.get(id=tt_volume.request_id)
        request.from_field = request.recipients = 'example@example.com'
        to_serialize.add(request)

        for event in Eventloggers.objects.filter(bar_code=tt_volume.bar_code):
            if not event.updated_at:
                event.updated_at = event.created_at
            to_serialize.add(event)
            user_ids.add(event.created_by)
            if event.pstep_id:
                pstep = Pstep.objects.get(step_id=event.pstep_id)
                to_serialize.add(pstep)

    for i, user in enumerate(Users.objects.filter(id__in=user_ids)):
        user.email = "example%s@example.com" % i
        user.password = 'password'
        user.remember_token = ''
        to_serialize.add(user)

    serializer = serializers.get_serializer("json")()
    with open(os.path.join(settings.BASE_DIR, "test_data/tracking_tool.json"), "w") as out:
        serializer.serialize(to_serialize, stream=out, indent=2)

    ## update inventory files
    write_inventory_files()

@task
def zip_jurisdiction(jurname, zip_filename=None):
    """
    Write a zipped directory of all case xml files in a given jurisdiction.
    The uncompressed output's structure looks like: jurisdiction/reporter_name/volume_number/case.xml
    """
    jurisdiction = Jurisdiction.objects.get(name=jurname)
    zip_filename = zip_filename if zip_filename else jurname + ".zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as archive:
        cases = CaseMetadata.objects.filter(jurisdiction=jurisdiction)

        for case in cases:
            reporter = case.reporter.short_name
            volume = case.volume.volume_number
            filename = case.slug + '.xml'
            path = "{0}/{1}/{2}".format(reporter, volume, filename)
            archive.writestr(path, resources.get_matching_case_xml(case.case_id))

    print("completed: jurisdiction " + jurname + ", zip filename " + zip_filename)



@task
def write_inventory_files():
    """ Create inventory.csv.gz files in test_data/inventory/data. Should be re-run if test_data/from_vendor changes. """

    # get list of all files in test_data/from_vendor
    results = []
    for dir_name, subdir_list, file_list in os.walk(os.path.join(settings.BASE_DIR, 'test_data/from_vendor')):
        for file_path in file_list:
            if file_path == '.DS_Store':
                continue
            file_path = os.path.join(dir_name, file_path)

            # for each file, get list of [bucket, path, size, mod_time, md5, multipart_upload]
            results.append([
                'harvard-ftl-shared',
                file_path[len(os.path.join(settings.BASE_DIR, 'test_data/')):],
                os.path.getsize(file_path),
                datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                hashlib.md5(open(file_path, 'rb').read()).hexdigest(),
                'FALSE',
            ])

    # write results, split in half, to two inventory files named test_data/inventory/data/1.csv.gz and test_data/inventory/data/2.csv.gz
    for out_name, result_set in (("1", results[:len(results)//2]), ("2", results[len(results)//2:])):
        with gzip.open(os.path.join(settings.BASE_DIR, 'test_data/inventory/data/%s.csv.gz' % out_name), "wt") as f:
            csv_w = csv.writer(f)
            for row in result_set:
                csv_w.writerow(row)


@task
def compress_volumes(*barcodes, max_volumes=10, delay=False):
    from capdb.storages import ingest_storage
    from capdb.tasks import compress_volume

    max_volumes = int(max_volumes)

    def get_volumes():
        """ Get all up-to-date volumes. """
        volumes = ingest_storage.iter_files("")
        current_vol = next(volumes, "")
        while current_vol:
            next_vol = next(volumes, "")
            if current_vol.split("_", 1)[0] != next_vol.split("_", 1)[0]:
                yield current_vol
            current_vol = next_vol

    if barcodes:
        # get folder for each barcode provided at command line
        barcodes = [max(ingest_storage.iter_files(barcode, partial_path=True)) for barcode in barcodes]
    else:
        # get latest folder all volumes
        barcodes = get_volumes()

    for i, barcode in enumerate(barcodes):
        if delay:
            compress_volume.delay(barcode)
        else:
            print("Calling", barcode)
            compress_volume.apply(args=[barcode])
        if max_volumes and i >= max_volumes:
            break

@task
def gz_dir(d, d2):
    import subprocess, shutil

    for root, dirs, files in os.walk(d):
        for f in files:
            f_in = os.path.join(root, f)
            f_out = f_in.replace(d, d2)
            os.makedirs(os.path.dirname(f_out), exist_ok=True)
            print("%s -> %s" % (f_in, f_out))
            if f.endswith('.xml'):
                subprocess.call("xz < %s > %s.gz" % (f_in, f_out), shell=True)
            else:
                shutil.copy(f_in, f_out)

@task
def count_gz_dir(d):
    size=0
    for root, dirs, files in os.walk(d):
        for f in files:
            if f.endswith('.gz'):
                size += os.path.getsize(os.path.join(root, f))
    print(size)