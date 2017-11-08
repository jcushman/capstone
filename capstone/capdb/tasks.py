import csv
import gzip
import hashlib
import logging
import tarfile
import tempfile
import os
from collections import defaultdict
from datetime import datetime
from multiprocessing.pool import ThreadPool
from celery import shared_task
import time
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil
import subprocess
import copy

from django.conf import settings

from capdb.storages import ingest_storage, zip_storage, get_storage
from scripts.helpers import copy_file, parse_xml, resolve_namespace

# logging
# disable boto3 info logging -- see https://github.com/boto/boto3/issues/521
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('nose').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
info = logger.info
info = print

# debugging
MAX_JP2_PER_ZIP = 0
MULTITHREADED = True

# separate function to check .tar integrity against volmets
# hash of .tar files
# convert .tiff to browser-friendly format
# encryption?

### HELPERS ###

def get_file_type(path):
    """ Sort volume files by type. """
    if '/alto/' in path:
        if path.endswith('.xml'):
            return 'alto'
        return None
    if '/images/' in path:
        if path.endswith('.jp2'):
            return 'jp2'
        if path.endswith('.tif'):
            return 'tif'
        return None
    if '/casemets/' in path:
        if path.endswith('.xml'):
            return 'case'
        return None
    if path.endswith('METS.md5'):
        return 'md5'
    if path.endswith('METS.xml'):
        return 'volume'
    return None

class HashingFile:
    """ File wrapper that stores a hash of the read or written data. """
    def __init__(self, source, hash_name='md5'):
        self._sig = hashlib.new(hash_name)
        self._source = source
        self.length = 0

    def read(self, *args, **kwargs):
        result = self._source.read(*args, **kwargs)
        self.update_hash(result)
        return result

    def write(self, value, *args, **kwargs):
        self.update_hash(value)
        return self._source.write(value, *args, **kwargs)

    def update_hash(self, value):
        self._sig.update(value)
        self.length += len(value)

    def hexdigest(self):
        return self._sig.hexdigest()

    def __getattr__(self, attr):
        return getattr(self._source, attr)

class LoggingTarFile(tarfile.TarFile):
    def addfile(self, tarinfo, fileobj=None):
        """
            Source copied from tarfile.TarFile, except for setting tarinfo.offset and tarinfo.offset_data.
        """
        self._check("awx")

        tarinfo = copy.copy(tarinfo)

        # NEW LINE
        tarinfo.offset = self.offset

        buf = tarinfo.tobuf(self.format, self.encoding, self.errors)
        self.fileobj.write(buf)
        self.offset += len(buf)

        # NEW LINE
        tarinfo.offset_data = self.offset

        # If there's data to follow, append it.
        if fileobj is not None:
            tarfile.copyfileobj(fileobj, self.fileobj, tarinfo.size)
            blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
            if remainder > 0:
                self.fileobj.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                blocks += 1
            self.offset += blocks * tarfile.BLOCKSIZE

        self.members.append(tarinfo)

def jp2_to_jpg(jp2_path, quality=40):
    """
        Convert jp2 at jp2_path to jpg and return jpg data.
        Required opj_decompress and mozcjpeg to be in PATH.
    """
    with tempfile.TemporaryDirectory() as tga_dir:
        tga_pipe = os.path.join(tga_dir, "pipe.tga")
        os.mkfifo(tga_pipe)

        try:
            proc = subprocess.Popen([
                "opj_decompress",
                "-i", jp2_path,
                "-o", tga_pipe,
                "-threads", "5",  # on a quick test, 5 threads seems to be fastest
                "-quiet",  # suppress progress messages
            ])
            proc2 = subprocess.Popen([
                "mozcjpeg",
                "-quality", str(quality),
                "-targa", tga_pipe
            ], stdout=subprocess.PIPE)
            out, err = proc2.communicate()
        finally:
            # just in case proc is hanging around for some reason
            proc.kill()

        if err:
            raise Exception("Error calling cjpeg: %s" % err)

        return out

### FILE HANDLERS ###

def handle_simple_file(volume_file_path, tempdir):
    ingest_storage, out_path = single_file_setup(volume_file_path, tempdir)
    copy_file(volume_file_path, str(out_path), from_storage=ingest_storage)

def handle_alto_file(volume_file_path, tempdir):
    ingest_storage, out_path = single_file_setup(volume_file_path, tempdir)

    out_path = out_path.with_suffix('.xml.gz')
    with ingest_storage.open(volume_file_path, "rb") as in_file:
        with out_path.open('wb') as out_file:
            out_file = HashingFile(out_file)
            with gzip.GzipFile(fileobj=out_file) as gz_file:
                shutil.copyfileobj(in_file, gz_file)

    return format_new_file_info(volume_file_path, out_path, out_file)

def handle_jp2_file(volume_file_path, tempdir):
    ingest_storage, out_path = single_file_setup(volume_file_path, tempdir)

    out_path = out_path.with_suffix('.jpg')
    start_time = time.time()
    with ingest_storage.open(volume_file_path, "rb") as im_file:
        data = im_file.read()

    # use opj_decompress to decode jp2 and mozjpeg to encode as jpg
    with tempfile.NamedTemporaryFile(suffix=".jp2") as jp2_file:
        jp2_file.write(data)
        jp2_file.flush()
        jpg_data = jp2_to_jpg(jp2_file.name)

    with out_path.open("wb") as out_file:
        out_file = HashingFile(out_file)
        out_file.write(jpg_data)
    info("Compress time:", time.time() - start_time)

    return format_new_file_info(volume_file_path, out_path, out_file)

def handle_mets_file(volume_file_path, tempdir, new_file_info, relative_path_prefix=''):
    ingest_storage, out_path = single_file_setup(volume_file_path, tempdir)

    with ingest_storage.open(volume_file_path, "r") as in_file:
        mets_xml = parse_xml(in_file.read())

    # add provenance data
    # spacing at start and end of string matters here -- makes sure formatting matches surrounding elements
    mets_xml('mets|amdSec').append("""  <digiprovMD ID="digi004">
      <mdWrap MDTYPE="PREMIS">
        <xmlData>
          <event xmlns="info:lc/xmlns/premis-v2">
            <eventIdentifier>
              <eventIdentifierType>Local</eventIdentifierType>
              <eventIdentifierValue>proc0001</eventIdentifierValue>
            </eventIdentifier>
            <eventType>compression</eventType>
            <eventDateTime>%s</eventDateTime>
            <eventDetail>File compression</eventDetail>
          </event>
          <agent xmlns="info:lc/xmlns/premis-v2">
            <agentIdentifier>
              <agentIdentifierType>Local</agentIdentifierType>
              <agentIdentifierValue>HLSL</agentIdentifierValue>
            </agentIdentifier>
            <agentName>Harvard Law School Library</agentName>
            <agentType>organization</agentType>
          </agent>
        </xmlData>
      </mdWrap>
    </digiprovMD>
  """ % (datetime.utcnow().isoformat().split('.')[0]+'Z'))

    # update <fileGrp> sections
    def fix_file_group(group_name, new_mime_type, new_id_prefix=None):
        file_group = mets_xml('mets|fileGrp[USE="%s"]' % group_name)
        for file_el in file_group('mets|file'):
            file_el = parse_xml(file_el)
            flocat_el = file_el('mets|FLocat')
            old_href = flocat_el.attr(resolve_namespace('xlink|href'))
            new_data = new_file_info[old_href.replace(relative_path_prefix, '')]

            if new_id_prefix:
                file_el.attr('ID', file_el.attr('ID').replace(group_name, new_id_prefix))
            file_el.attr('MIMETYPE', new_mime_type)
            file_el.attr('CHECKSUM', new_data['digest'])
            file_el.attr('SIZE', str(new_data['length']))

            flocat_el.attr(resolve_namespace('xlink|href'), relative_path_prefix+new_data['new_path'])
        if new_id_prefix:
            file_group.attr('USE', new_id_prefix)
    fix_file_group('jp2', 'image/jpg', 'jpg')
    fix_file_group('alto', 'text/xml+gzip')
    fix_file_group('casemets', 'text/xml+gzip')

    # fix <fptr> elements
    for fptr in mets_xml('mets|fptr'):
        fileid = fptr.attrib.get('FILEID', '')
        if fileid.startswith('jp2'):
            fptr.attrib['FILEID'] = fileid.replace('jp2', 'jpg')

    # write out xml
    out_path = out_path.with_suffix('.xml.gz')
    with out_path.open('wb') as out_file:
        out_file = HashingFile(out_file)
        with gzip.GzipFile(fileobj=out_file) as gz_file:
            gz_file.write(str(mets_xml).encode('utf8'))

    return format_new_file_info(volume_file_path, out_path, out_file)

def handle_md5_file(volume_file_path, tempdir, new_digest):
    ingest_storage, out_path = single_file_setup(volume_file_path, tempdir)

    with out_path.open('w') as out:
        out.write(new_digest)

def format_new_file_info(volume_file_path, out_path, out_file):
    return volume_file_path, {'new_path': str(out_path), 'digest':out_file.hexdigest(), 'length':out_file.length}

def single_file_setup(volume_file_path, tempdir):
    info("processing %s" % volume_file_path)
    ingest_storage = get_storage('ingest_storage')  # start new connection for threads
    out_path = tempdir / volume_file_path
    out_path.parents[0].mkdir(parents=True, exist_ok=True)
    return ingest_storage, out_path

@shared_task
def compress_volume(volume_name):
    info("listing volume")

    # only create archive if it doesn't already exist
    archive_name = volume_name + ".tar"
    if zip_storage.exists(archive_name):
        info("%s already exists, returning" % volume_name)
        return

    # get sorted files
    volume_files = ingest_storage.iter_files_recursive(volume_name)
    volume_files_by_type = defaultdict(list)
    for volume_file in volume_files:
        volume_files_by_type[get_file_type(volume_file)].append(volume_file)

    # make a copy of volume in a temp dir
    with TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)
        volume_path = tempdir / volume_name
        volume_path.mkdir()

        # set up multithreading -- file_map function lets us run function in parallel across a list of file paths,
        # passing in tempdir along with each file path.
        if MULTITHREADED:
            pool = ThreadPool(20)
            mapper = pool.map
        else:
            mapper = map
        def file_map(func, files, *args, **kwargs):
            return list(mapper((lambda f: func(f, tempdir, *args, **kwargs)), files))

        # store mapping of old paths to new paths and related md5 info
        new_file_info = {}
        def add_file_info(info):
            for k, v in info:
                k = os.path.relpath(k, volume_name)
                new_file_info[k] = v
                v['new_path'] = os.path.relpath(v['new_path'], str(volume_path))

        # write alto, tif, and jpg files
        jpg_file_results = file_map(handle_jp2_file, volume_files_by_type.get('jp2', []))
        tif_file_results = file_map(handle_simple_file, volume_files_by_type.get('tif', []))
        alto_file_results = file_map(handle_alto_file, volume_files_by_type.get('alto', []))

        # write casemets files, using data gathered in previous step
        add_file_info(jpg_file_results)
        add_file_info(alto_file_results)
        case_file_results = file_map(handle_mets_file, volume_files_by_type.get('case', []), new_file_info, '../')

        # write volmets file, using data gathered in previous step
        add_file_info(case_file_results)
        new_volume_info = handle_mets_file(volume_files_by_type['volume'][0], tempdir, new_file_info)

        # finally write volmets md5
        handle_md5_file(volume_files_by_type['md5'][0], tempdir, new_volume_info[1]['digest'])

        # tar volume
        info("tarring %s" % volume_path)
        with zip_storage.open(archive_name, "wb") as out:
            tar = LoggingTarFile.open(fileobj=out, mode='w|')
            try:
                tar.add(str(volume_path), volume_name)
                with zip_storage.open(archive_name+".csv", "w") as csv_out:
                    csv_writer = csv.writer(csv_out)
                    csv_writer.writerow(["path", "offset", "size"])
                    for member in tar.members:
                        csv_writer.writerow([member.name, member.offset_data, member.size])
            finally:
                tar.close()

@shared_task
def validate_volume(volume_name):
    pass