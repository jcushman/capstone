import bisect
import difflib
import json
import re
import tempfile
from base64 import b64encode
from copy import deepcopy
from io import BytesIO
from zipfile import ZipFile
import nacl.encoding
import nacl.secret
from pathlib import Path
from PIL import Image
from celery import shared_task
from lxml import etree

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from capdb.models import CaseFont, PageStructure, VolumeMetadata, CaseMetadata, CaseStructure, Reporter, \
    CaseInitialMetadata, CaseXML, TarFile
from capdb.storages import captar_storage
from scripts import render_case
from scripts.compress_volumes import files_by_type, open_captar_volume
from scripts.helpers import parse_xml


# TODO:
# - store ingested file path and md5

def write_json(obj, path):
    s = json.dumps(obj, indent=2)
    path.write_text(s)

def read_json(path):
    return json.loads(path.read_text())

rect_attrs = ['HPOS', 'VPOS', 'WIDTH', 'HEIGHT']
def rect(el_attrib):
    return [float(el_attrib[attr]) if '.' in el_attrib[attr] else int(el_attrib[attr]) for attr in rect_attrs]

def parse(storage, path, remove_namespaces=True):
    xml = parse_xml(storage.contents(path[:-3]))
    if remove_namespaces:
        xml = xml.remove_namespaces()
    return xml

def get_tokens(parent):
    yield parent[0].text
    for el in parent.children().items():
        yield [el[0].tag] + ([dict(el[0].attrib)] if el[0].attrib else [])
        yield from get_tokens(el)
        yield ['/'+el[0].tag]
        yield el[0].tail

def insert_tags(block, i, offset, new_tokens):
    text = block[i]
    to_insert = [token for token in [text[:offset]]+new_tokens+[text[offset:]] if token]
    block[i:i + 1] = to_insert
    return len(to_insert) - 1

def index_blocks(blocks):
    blocks_text = ''
    blocks_offsets = []
    blocks_lookup = []
    for block in blocks:
        for i, token in enumerate(block):
            if type(token) == str:
                blocks_offsets.append(len(blocks_text))
                blocks_lookup.append((len(blocks_text), block, i))
                blocks_text += token
    return blocks_text, blocks_offsets, blocks_lookup

def add_tags(blocks, new_tokens):
    """
    blocks = [[["baz"], "abc-", "def"], ["gh-i", "jkl", ["/baz"]]]
    new_tokens = [["bar"], "abcd", ["foo"], "efg", ['/foo'], "hijkl", ["/bar"]]
    add_tags(blocks, new_tokens)
    assert blocks == [[['baz'], ['bar'], 'abc', ['edit', {'was': '-'}], ['/edit'], 'd', ['foo'], 'ef'], [['edit', {'was': '-'}], ['/edit'], 'g', ['/foo'], 'hi', 'jkl', ['/bar'], ['/baz']]]
    """
    if not blocks:
        # could be empty if paragraph consists solely of images
        return

    # Break out the new tokens into a list of non-text tokens in [offset, token] form, and a string of the text tokens
    case_tokens = []
    case_text = ""
    for token in new_tokens:
        if type(token) == str:
            if not case_text:
                # lstrip up here so offsets will be correct
                case_text += token.lstrip()
            else:
                case_text += token
        else:
            case_tokens.append([len(case_text), token])
    case_text = case_text.rstrip()  # remove any whitespace at start and end of paragraph of case text

    # Break out the blocks into string of text tokens, list of offsets for non-text tokens, and list of non-text tokens
    # in (offset, token_list, index) form
    blocks_text, blocks_offsets, blocks_lookup = index_blocks(blocks)

    # Find diff between two text strings, and update offsets in case_tokens to match text in blocks_text
    if case_text != blocks_text:
        diff = difflib.SequenceMatcher(None, blocks_text, case_text)
        opcodes = reversed([opcode for opcode in diff.get_opcodes() if opcode[0] != "equal"])
        for tag, i1, i2, j1, j2 in opcodes:
            block_index = bisect.bisect_right(blocks_offsets, i1) - 1
            offset, block, i = blocks_lookup[block_index]
            i1 -= offset
            i2 -= offset
            needed = i2-i1
            if needed:
                # tag == "replace" or "delete"
                removed = block[i][i1:i2]
                block[i] = block[i][:i1] + block[i][i2:]
            else:
                # tag == "insert"
                removed = ''
            insert_count = insert_tags(block, i, i1, [
                ['edit', {'was': removed}],
                case_text[j1:j2],  # will be empty if tag == "delete"
                ['/edit'],
            ])

            # for replace or delete, keep removing from subsequent strings if necessary
            needed -= len(removed)
            while needed > 0:
                old_block = block
                block_index += 1
                offset, block, i = blocks_lookup[block_index]
                if block != old_block:
                    insert_count = 0
                i += insert_count
                removed = block[i][:needed]
                block[i] = block[i][needed:]
                insert_count += insert_tags(block, i, 0, [
                    ['edit', {'was': removed}],
                    ['/edit'],
                ])
                needed -= len(removed)

        # update offsets
        blocks_text, blocks_offsets, blocks_lookup = index_blocks(blocks)

    # Insert all case_tokens into blocks
    for position, token in reversed(case_tokens):
        offset, block, i = blocks_lookup[bisect.bisect_right(blocks_offsets, position)-1]
        offset = position-offset
        text = block[i]
        block[i:i+1] = ([text[:offset]] if offset > 0 else []) + [token] + ([text[offset:]] if offset <= len(text) - 1 else [])

def add_tags_for_case_pars(pars, blocks_by_id, case_id_to_alto_ids):
    for par in pars:
        alto_ids = case_id_to_alto_ids[par.attr.id]
        blocks = [blocks_by_id[id]['tokens'] for id in alto_ids if blocks_by_id[id].get('tokens')]
        for block in blocks[:-1]:
            # add space to last word in block
            for i in range(len(block) - 1, -1, -1):
                if type(block[i]) == str:
                    block[i] = block[i].rstrip() + ' '
                    break
        add_tags(blocks, get_tokens(par))

def extract_paragraphs(children, case_id_to_alto_ids, blocks_by_id):
    paragraphs = []
    footnotes = []
    paragraph_els = []
    for el in children:
        if el[0].tag == 'footnote':
            footnote = {'id': el.attr.id}
            if el.attr.label:
                footnote['label'] = el.attr.label
            if el.attr.redact:
                footnote['redacted'] = el.attr.redact == 'true'
            if el.attr.orphan:
                footnote['orphan'] = el.attr.orphan
            footnote_pars, _, footnote_par_els = extract_paragraphs(el.children().items(), case_id_to_alto_ids, blocks_by_id)
            footnote['paragraphs'] = footnote_pars
            paragraph_els.extend(footnote_par_els)
            footnotes.append(footnote)
        else:
            par = {
                'id': el.attr.id,
                'class': el[0].tag,
                'block_ids': case_id_to_alto_ids[el.attr.id],
            }
            if all(blocks_by_id[id].get('redacted', False) for id in par['block_ids']):
                par['redacted'] = True
            paragraphs.append(par)
            paragraph_els.append(el)
    return paragraphs, footnotes, paragraph_els

def iter_case_paragraphs(opinions):
    for opinion in opinions:
        yield from opinion.get('paragraphs', [])
        for footnote in opinion.get('footnotes', []):
            yield from footnote.get('paragraphs', [])

@shared_task
def volume_to_json(volume_barcode, primary_path, secondary_path, key=settings.REDACTION_KEY):
    with open_captar_volume(primary_path) as unredacted_storage:
        if secondary_path:
            with open_captar_volume(secondary_path) as redacted_storage:
                volume_to_json_inner(volume_barcode, unredacted_storage, redacted_storage, key=key)
        else:
            volume_to_json_inner(volume_barcode, unredacted_storage, key=key)

def volume_to_json_inner(volume_barcode, unredacted_storage, redacted_storage=None, key=settings.REDACTION_KEY):

    pages = []
    cases = []
    fonts = {}
    paths = files_by_type(sorted(unredacted_storage.iter_files_recursive()))

    # handle volmets
    print("Reading volmets")
    parsed = parse(unredacted_storage, paths['volume'][0])
    volume_el = parsed('volume')
    volume = {
        'barcode': volume_el.attr.barcode,
        'page_labels': {int(item.attr.ORDER): item.attr.ORDERLABEL for item in parsed('structMap[TYPE="physical"] div[TYPE="volume"] div[TYPE="page"]').items()},
    }
    metadata = volume['metadata'] = {}
    reporter_el = volume_el('reporter')
    metadata['reporter'] = {
        'name': reporter_el.text(),
        'abbreviation': reporter_el.attr.abbreviation,
        'volume_number': reporter_el.attr.volnumber,
    }
    nominative_reporters = [{
       'name': reporter_el.text(),
        'abbreviation': reporter_el.attr.abbreviation,
        'volume_number': reporter_el.attr.volnumber,
    } for reporter_el in volume_el('nominativereporter').items() if (
        reporter_el.text() or reporter_el.attr.abbreviation or reporter_el.attr.volnumber
    )]
    if nominative_reporters:
        metadata['nominative_reporters'] = nominative_reporters
    metadata['start_date'] = volume_el('voldate start').text()
    metadata['end_date'] = volume_el('voldate end').text()
    metadata['spine_start_date'] = volume_el('spinedate start').text()
    metadata['spine_end_date'] = volume_el('spinedate end').text()
    metadata['publication_date'] = volume_el('publicationdate').text()
    publisher_el = volume_el('publisher')
    metadata['publisher'] = {
        'name': publisher_el.text(),
        'place': publisher_el.attr.place,
    }
    metadata['tar_path'] = str(unredacted_storage.path)
    metadata['tar_hash'] = unredacted_storage.get_hash()

    # handle alto
    print("Reading ALTO files")
    fake_font_id = 1
    fonts_by_id = {}
    for path in paths['alto']:
        print(path)

        parsed = parse(unredacted_storage, path)
        alto_id = 'alto_' + path.split('_ALTO_', 1)[1].split('.')[0]

        # handle redaction
        if redacted_storage:
            redacted_parsed = parse(redacted_storage, path.replace('unredacted', 'redacted'))
            unredacted_block_ids = {block.attr.ID for block in redacted_parsed('TextBlock,Illustration').items()}
            unredacted_string_ids = {block.attr.ID for block in redacted_parsed('String').items()}

        tags = {s.attrib['ID']: s.attrib['LABEL'] for s in parsed[0].iter('StructureTag')}  #

        page_el = parsed('Page')
        page = {
            'id': alto_id,
            'path': str(path),
            'order': int(page_el.attr.ID.split("_")[1]),
            'width': int(page_el.attr.WIDTH),
            'height': int(page_el.attr.HEIGHT),
            'file_name': parsed('sourceImageInformation fileName').text().replace('.png', '.tif'),
            'deskew': parsed('processingStepSettings').text(),
            'spaces': [],
            'blocks': [],
        }
        page['label'] = volume['page_labels'][page['order']]

        # look up PageStyle entries in DB
        fonts_for_page = {}
        for s in parsed[0].iter('TextStyle'):
            font = (s.attrib['FONTFAMILY'], s.attrib['FONTSIZE'], s.attrib.get('FONTSTYLE', ''), s.attrib['FONTTYPE'], s.attrib['FONTWIDTH'])
            if font in fonts:
                font_id = fonts[font]
            else:
                font_obj = CaseFont(pk=fake_font_id, family=font[0], size=font[1], style=font[2], type=font[3], width=font[4])
                fake_font_id += 1
                font_id = fonts[font] = font_obj.pk
                fonts_by_id[font_id] = font_obj
            fonts_for_page[s.attrib['ID']] = font_id
        page['font_names'] = {v:k for k, v in fonts_for_page.items()}  # used for confirming reversability

        for space_el in page_el('PrintSpace').items():
            space_rect = rect(space_el[0].attrib)
            if space_rect == [0, 0, page['width'], page['height']]:
                space_index = None
            else:
                page['spaces'].append(space_rect)
                space_index = len(page['spaces']) - 1

            for block_el in space_el[0]:
                block = {
                    'id': block_el.attrib['ID'],
                    'rect': rect(block_el.attrib),
                    'class': tags[block_el.attrib['TAGREFS']],
                }
                if space_index is not None:
                    block['space'] = space_index
                if redacted_storage and block['id'] not in unredacted_block_ids:
                    block['redacted'] = True
                if block_el.tag == 'Illustration':
                    # read image data
                    block['format'] = 'image'
                    with unredacted_storage.open('images/%s' % page['file_name']) as in_file:
                        with tempfile.NamedTemporaryFile(suffix='.tif') as temp_img:
                            temp_img.write(in_file.read())
                            temp_img.flush()
                            img = Image.open(temp_img.name)
                            img.load()
                    r = block['rect']
                    cropped_img = img.crop((r[0], r[1], r[0]+r[2], r[1]+r[3]))
                    png_data = BytesIO()
                    cropped_img.save(png_data, 'PNG')
                    block['data'] = 'image/png;base64,' + b64encode(png_data.getvalue()).decode('utf8')
                else:
                    tokens = block['tokens'] = []
                    last_font = None
                    redacted_span = False
                    check_string_redaction = redacted_storage and not block.get('redacted', False)
                    for line_el in block_el.iter('TextLine'):
                        tokens.append(['line', {'rect': rect(line_el.attrib)}])
                        for string in line_el.iter('String'):
                            string_attrib = string.attrib  # for speed

                            # handle font
                            font = string_attrib['STYLEREFS']
                            if font != last_font:
                                if last_font:
                                    tokens.append(['/font'])
                                tokens.append(['font', {'id': fonts_for_page[font]}])
                                last_font = font

                            # handle redaction
                            if check_string_redaction:
                                if redacted_span:
                                    if string_attrib['ID'] in unredacted_string_ids:
                                        tokens.append(['/redact'])
                                        redacted_span = False
                                else:
                                    if string_attrib['ID'] not in unredacted_string_ids:
                                        tokens.append(['redact'])
                                        redacted_span = True

                            ocr_token = ['ocr', {'rect': rect(string_attrib), 'wc': float(string_attrib['WC'])}]
                            cc = string_attrib['CC']
                            if '9' in cc:
                                ocr_token[1]['cc'] = int(cc.replace('9', '1'), 2)

                            text = string_attrib['CONTENT']
                            next_tag = string.getnext()
                            if (next_tag is not None and next_tag.tag == 'SP') or text[-1] not in ('-', '\xad'):
                                text += ' '

                            tokens.extend((ocr_token, text, ['/ocr']))
                        tokens.append(['/line'])

                    # remove space from last word in block
                    for i in range(len(tokens)-1, -1, -1):
                        if type(tokens[i]) == str:
                            tokens[i] = tokens[i].rstrip()
                            break

                    # close open spans
                    if last_font:
                        tokens.append(['/font'])
                    if redacted_span:
                        tokens.append(['/redact'])

                page['blocks'].append(block)
        pages.append(page)

    # handle cases
    print("Reading Case files")
    blocks_by_id = {block['id']: block for page in pages for block in page['blocks']}
    for path in paths['case']:
        print(path)

        parsed = parse(unredacted_storage, path, remove_namespaces=False)

        # check duplicative
        duplicative = bool(parsed('duplicative|casebody'))
        parsed = parsed.remove_namespaces()
        casebody = parsed('casebody')
        case = {'path': str(path)}
        if duplicative:
            case_number = parsed('fileGrp[USE="casebody"] > file').attr.ID.split('_')[1]
            case['id'] = '%s_%s' % (volume_barcode, case_number)
            metadata = case['metadata'] = {'duplicative': True}

        else:
            # reorder head matter
            ids_before_reorder = {el.attr.id for el in casebody('[id]').items()}
            CaseXML.reorder_head_matter(parsed)
            ids_after_reorder = {el.attr.id for el in casebody('[id]').items()}
            if ids_before_reorder != ids_after_reorder:
                raise Exception("reorder_head_matter id mismatch for %s" % path)

            ## extract <case> element
            case_el = parsed('case')
            case['id'] = case_el.attr.caseid
            metadata = case['metadata'] = {
                'duplicative': False,
                'status': case_el.attr.publicationstatus,
                'decision_date': case_el('decisiondate').text(),
            }
            court = case_el('court')
            metadata['court'] = {
                'abbreviation': court.attr.abbreviation,
                'jurisdiction': court.attr.jurisdiction,
                'name': court.text(),
            }
            district = case_el('district')
            if district.length:
                metadata['district'] = {
                    'name': district.text(),
                    'abbreviation': district.attr.abbreviation,
                }
            argument_date = case_el('argumentdate')
            if argument_date.length:
                metadata['argument_date'] = argument_date.text()
            name = case_el('name')
            metadata['name'] = name.text()
            metadata['name_abbreviation'] = name.attr.abbreviation
            metadata['docket_numbers'] = [el.text() for el in case_el('docketnumber').items() if el.text()]
            metadata['citations'] = [{
                'category': cite.attr.category,
                'type': cite.attr.type,
                'text': cite.text(),
            } for cite in case_el('citation').items()]

        # handle blocks
        case_id_to_alto_ids = {}
        for xref_el in parsed('div[TYPE="blocks"] > div[TYPE="element"]').items():
            par_el, blocks_el = xref_el('fptr').items()
            case_id_to_alto_ids[par_el('area').attr.BEGIN] = [block_el.attr.BEGIN for block_el in blocks_el('area').items()]

        # handle casebody
        metadata['first_page'] = casebody.attr.firstpage
        metadata['last_page'] = casebody.attr.lastpage

        if duplicative:
            sections = [['unprocessed', casebody.children().items()]]
        else:
            opinion_els = [[el.attr.type, list(el.children().items())] for el in casebody.children('opinion').items()]
            head_matter_children = casebody.children(':not(opinion):not(corrections)').items()
            sections = [['head', list(head_matter_children)]] + opinion_els

            # link <bracketnum> to <headnotes>
            headnotes_lookup = {}
            for headnote in casebody('headnotes').items():
                number = re.match(r'\d+', headnote.text())
                if number:
                    headnotes_lookup[number.group(0)] = headnote.attr.id
            for headnote_ref in casebody('bracketnum').items():
                number = re.search(r'\d', headnote_ref.text())
                if number and number.group(0) in headnotes_lookup:
                    headnote_ref.attr.ref = headnotes_lookup[number.group(0)]

        case['opinions'] = opinions = []
        for footnote_opinion_index, (op_type, children) in enumerate(sections):

            # link <footnotemark> to <footnote>
            if not duplicative:
                footnote_index = 1
                footnotes_lookup = {}
                for tag in children:
                    if tag[0].tag == 'footnote':
                        footnote_id = 'footnote_%s_%s' % (footnote_opinion_index, footnote_index)
                        tag.attr('id', footnote_id)
                        if tag.attr('label'):
                            footnotes_lookup[tag.attr('label')] = footnote_id
                        footnote_index += 1
                for tag in children:
                    for footnote_ref in tag.find('footnotemark').items():
                        if footnote_ref.text() in footnotes_lookup:
                            footnote_ref.attr.ref = footnotes_lookup[footnote_ref.text()]

            opinion = {
                'type': op_type,
            }
            paragraphs, footnotes, paragraph_els = extract_paragraphs(children, case_id_to_alto_ids, blocks_by_id)
            add_tags_for_case_pars(paragraph_els, blocks_by_id, case_id_to_alto_ids)
            if paragraphs:
                opinion['paragraphs'] = paragraphs
            if footnotes:
                opinion['footnotes'] = footnotes
            opinions.append(opinion)

        # handle corrections
        if not duplicative:
            corrections = list(case_el.children('correction').items())
            if corrections:
                case['corrections'] = [el.text() for el in corrections]

        cases.append(case)

    # write_json(cases, cases_cache)

    if redacted_storage:
        print("Encrypting pages")
        for page in pages:
            encrypt_page(page, key)
        print("Decrypting pages")
        decrypted_pages = deepcopy(pages)
        for page in decrypted_pages:
            decrypt_page(page, key, delete_encrypted_strings=True)
        blocks_by_id = {block['id']: block for page in decrypted_pages for block in page['blocks']}
    else:
        decrypted_pages = pages

    ### validate results
    renderer = render_case.VolumeRenderer(blocks_by_id, fonts_by_id)

    # validate volume
    volume_obj = VolumeMetadata(barcode=volume_barcode, xml_metadata=volume['metadata'])
    new_xml = renderer.render_volume(volume_obj)
    parsed = parse(unredacted_storage, paths['volume'][0])
    volume_el = parsed('volume')
    old_xml = str(volume_el)
    old_xml = old_xml.replace('<nominativereporter abbreviation="" volnumber=""/>', '')
    xml_strings_equal(new_xml, old_xml)

    # validate alto
    print("Checking ALTO integrity")
    for page in decrypted_pages:
        to_test = [(unredacted_storage, page['path'], False)]
        if redacted_storage:
            to_test.append((redacted_storage, page['path'].replace('unredacted', 'redacted'), True))
        page_obj = create_page_obj(volume_obj, page)
        for storage, path, redacted in to_test:
            print("- checking %s" % path)
            parsed = parse(storage, path)
            alto_xml_output = renderer.render_page(page_obj, redacted)
            original_alto = str(parsed('Page'))
            original_alto = original_alto.replace('WC="1.0"', 'WC="1.00"')  # normalize irregular decimal places
            xml_strings_equal(alto_xml_output, original_alto, {
                'tag_attrs': {
                    'Page': {'ID', 'PHYSICAL_IMG_NR', 'xmlns'},
                    'PrintSpace': {'ID'},
                    'TextBlock': {'TAGREFS'},
                    'Illustration': {'TAGREFS'},
                    'TextLine': {'ID'},
                    'String': {'ID', 'TAGREFS'},
                    'SP': {'HPOS', 'ID', 'VPOS', 'WIDTH'}
                }
            })

    # validate cases
    print("Checking case integrity")
    for case in cases:
        case_obj = create_case_obj(volume_obj, case)
        to_test = [(unredacted_storage, case['path'], False)]
        if redacted_storage:
            to_test.append((redacted_storage, case['path'].replace('unredacted', 'redacted'), True))
        for storage, path, redacted in to_test:
            print("- checking %s" % path)
            parsed = parse(storage, path)
            CaseXML.reorder_head_matter(parsed)
            case_xml_equal(case_obj, renderer, parsed, redacted)

    # write out results
    out_path = Path('token_streams', unredacted_storage.path.name+'.zip')
    print("Writing temp files to %s" % out_path)
    font_attrs = ['family', 'size', 'style', 'type', 'width']
    fonts_by_id = {f.id: {attr: getattr(f, attr) for attr in font_attrs} for f in fonts_by_id.values()}
    if captar_storage.exists(str(out_path)):
        captar_storage.delete(str(out_path))
    with tempfile.SpooledTemporaryFile(max_size=2**20*100) as tmp:
        with ZipFile(tmp, 'w') as zip:
            for path, obj in (('volume.json', volume), ('pages.json', pages), ('cases.json', cases), ('fonts.json', fonts_by_id)):
                zip.writestr(path, json.dumps(obj))
        tmp.seek(0)
        captar_storage.save(str(out_path), tmp)

### write to database
def create_page_obj(volume_obj, page, ingest_source=None):
    return PageStructure(
        volume=volume_obj,
        order=page['order'],
        label=page['label'],
        blocks=page['blocks'],
        spaces=page['spaces'] or None,
        image_file_name=page['file_name'],
        width=page['width'],
        height=page['height'],
        deskew=page['deskew'],
        font_names=page['font_names'],
        ingest_source=ingest_source,
        ingest_path=page['path'],
    )

def create_case_obj(volume_obj, case):
    metadata_obj = CaseMetadata(case_id=case['id'],
                                # reporter=volume_obj.reporter,
                                volume=volume_obj, duplicative=case['metadata']['duplicative'],
                                first_page=case['metadata']['first_page'], last_page=case['metadata']['last_page'])
    metadata_obj.structure = CaseStructure(metadata=metadata_obj, opinions=case['opinions'], corrections=case.get('corrections') or None)
    metadata_obj.initial_metadata = CaseInitialMetadata(case=metadata_obj, metadata=case['metadata'])
    return metadata_obj

@shared_task
def write_to_db(volume_barcode, zip_path):
    print("Loading data for volume %s from %s" % (volume_barcode, zip_path))
    with captar_storage.open(zip_path, 'rb') as raw:
        with ZipFile(raw) as zip:
            volume = json.loads(zip.open('volume.json').read().decode('utf8'))
            pages = json.loads(zip.open('pages.json').read().decode('utf8'))
            cases = json.loads(zip.open('cases.json').read().decode('utf8'))
            fonts_by_id = json.loads(zip.open('fonts.json').read().decode('utf8'))

    with transaction.atomic(using='capdb'):

        # save volume
        try:
            volume_obj = VolumeMetadata.objects.get(barcode=volume_barcode)
        except VolumeMetadata.DoesNotExist:
            # fake up a volume for testing
            volume_obj = VolumeMetadata(barcode=volume_barcode)
            volume_obj.reporter, _ = Reporter.objects.get_or_create(short_name=volume['metadata']['reporter']['abbreviation'], defaults={'full_name': volume['metadata']['reporter']['name'], 'updated_at': timezone.now(), 'created_at': timezone.now(), 'hollis':[]})
        volume_obj.xml_metadata = volume['metadata']
        volume_obj.save()

        # save TarFile
        ingest_source, _ = TarFile.objects.get_or_create(storage_path=volume['metadata']['tar_path'], hash=volume['metadata']['tar_hash'])

        # create fonts
        print("Saving fonts")
        font_fake_id_to_real_id = {}
        for font_fake_id, font_attrs in fonts_by_id.items():
            font_obj, _ = CaseFont.objects.get_or_create(**font_attrs)
            font_fake_id_to_real_id[int(font_fake_id)] = font_obj.pk

        # update fake font IDs
        for page in pages:
            page['font_names'] = {font_fake_id_to_real_id[int(k)]:v for k,v in page['font_names'].items()}
            for block in page['blocks']:
                for token in block.get('tokens', []):
                    if type(token) != str and token[0] == 'font':
                        token[1]['id'] = font_fake_id_to_real_id[token[1]['id']]

        # clear existing imports
        # TODO: testing only?
        print("Deleting existing items")
        CaseStructure.objects.filter(metadata__volume=volume_obj).delete()
        CaseInitialMetadata.objects.filter(case__volume=volume_obj).delete()
        volume_obj.page_structures.all().delete()

        # save pages
        print("Saving pages")
        page_objs = PageStructure.objects.bulk_create(create_page_obj(volume_obj, page, ingest_source) for page in pages)

        # save cases
        print("Saving cases")
        case_objs = []
        initial_metadata_objs = []
        for case in cases:
            try:
                metadata_obj = CaseMetadata.objects.get(case_id=case['id'])
            except CaseMetadata.DoesNotExist:
                metadata_obj = CaseMetadata(case_id=case['id'], reporter=volume_obj.reporter, volume=volume_obj)
                metadata_obj.save()
            case_objs.append(CaseStructure(metadata=metadata_obj, opinions=case['opinions'], corrections=case.get('corrections') or None, ingest_source=ingest_source, ingest_path=case['path']))
            initial_metadata_objs.append(CaseInitialMetadata(case=metadata_obj, metadata=case['metadata'], ingest_source=ingest_source, ingest_path=case['path']))
        case_objs = CaseStructure.objects.bulk_create(case_objs)
        initial_metadata_objs = CaseInitialMetadata.objects.bulk_create(initial_metadata_objs)

        # save join table
        print("Saving join table")
        page_objs_by_block_id = {block['id']:p for p in page_objs for block in p.blocks}
        links = {(case.id, page_objs_by_block_id[block_id].id) for case in case_objs for par in iter_case_paragraphs(case.opinions) for block_id in par['block_ids']}
        link_objs = [CaseStructure.pages.through(casestructure_id=case_id, pagestructure_id=page_id) for case_id, page_id in links]
        CaseStructure.pages.through.objects.bulk_create(link_objs)

        # update cached values
        print("Caching data")
        for case in case_objs:
            # TODO: optimize to use existing data instead of fetching from DB
            case.metadata.sync_from_initial_metadata()
            case.metadata.sync_case_structure()


def case_xml_equal(case_obj, renderer, parsed, redacted):
    # compare <casebody>
    renderer.redacted = redacted
    new_casebody = renderer.render_orig_xml(case_obj)
    old_casebody = str(parsed('casebody'))

    # remove whitespace from start and end of these tags:
    strip_whitespace_els = 'blockquote|author|p|headnotes|history|disposition|syllabus|summary|attorneys|judges|otherdate|decisiondate|parties|seealso|citation|docketnumber|court'
    old_casebody = re.sub(r'(<(?:%s)[^>]*>)\s+' % strip_whitespace_els, r'\1', old_casebody, flags=re.S)
    old_casebody = re.sub(r'\s+(</(?:%s)>)' % strip_whitespace_els, r'\1', old_casebody, flags=re.S)
    old_casebody = old_casebody.replace(' label=""', '')  # footnote with empty label
    xml_strings_equal(new_casebody, old_casebody, {'attrs': {'pgmap', 'xmlns'}})

    # compare <case>
    if not case_obj.duplicative:
        new_case_head = renderer.render_case_header(case_obj.case_id, case_obj.initial_metadata.metadata)
        old_case_head = str(parsed('case'))
        old_case_head = old_case_head.replace('<docketnumber/>', '')
        xml_strings_equal(new_case_head, old_case_head)

def xml_strings_equal(s1, s2, ignore={}):
    """ Raise ValueError if xml strings do not represent equivalent XML, ignoring linebreaks (but not whitespace) between elements. """
    s1 = re.sub(r'>\s*\n\s*<', '><', s1, flags=re.S)
    s2 = re.sub(r'>\s*\n\s*<', '><', s2, flags=re.S)
    e1 = etree.fromstring(s1)
    e2 = etree.fromstring(s2)
    return elements_equal(e1, e2, ignore)

def elements_equal(e1, e2, ignore={}):
    if e1.tag != e2.tag: raise ValueError("%s != %s" % (e1, e2))
    if e1.text != e2.text: raise ValueError("%s != %s" % (e1, e2))
    if e1.tail != e2.tail: raise ValueError("%s != %s" % (e1, e2))
    ignore_attrs = ignore.get('attrs', set()) | ignore.get('tag_attrs', {}).get(e1.tag.rsplit('}', 1)[-1], set())
    if {k:v for k,v in e1.attrib.items() if k not in ignore_attrs} != {k:v for k,v in e2.attrib.items() if k not in ignore_attrs}: raise ValueError("%s != %s" % (e1, e2))
    s1 = [i for i in e1 if i.tag.rsplit('}', 1)[-1] not in ignore.get('tags', ())]
    s2 = [i for i in e2 if i.tag.rsplit('}', 1)[-1] not in ignore.get('tags', ())]
    if len(s1) != len(s2): raise ValueError("%s != %s" % (e1, e2))
    for c1, c2 in zip(s1, s2):
        elements_equal(c1, c2, ignore)


def encrypt_page(page, key=settings.REDACTION_KEY):
    if 'encrypted_strings' in page:
        raise ValueError("Cannot encrypt page with existing 'encrypted_strings' key.")

    # extract each redacted string and replace it with a reference like ['enc', {'i': 1}]. i value will later become the
    # index into an encrypted array of strings.
    strings = {}
    string_counter = 0
    for block in page['blocks']:
        if block.get('format') == 'image':
            if block.get('redacted', False):
                enc_data = strings[block['data']] = ['enc', {'i': string_counter}]
                block['data'] = enc_data
                string_counter += 1
        else:
            all_redacted = block.get('redacted', False)
            redacted_span = False
            tokens = block.get('tokens', [])
            for i in range(len(tokens)):
                token = tokens[i]
                if type(token) == str:
                    if all_redacted or redacted_span:
                        if token not in strings:
                            strings[token] = ['enc', {'i': string_counter}]
                            string_counter += 1
                        tokens[i:i+1] = [strings[token]]
                elif token[0] == 'redact':
                    redacted_span = True
                elif token[0] == '/redact':
                    redacted_span = False

    # update references with correct i values
    string_vals = list(strings.keys())
    for i, val in enumerate(string_vals):
        strings[val][1]['i'] = i

    # store encrypted array
    box = nacl.secret.SecretBox(key, encoder=nacl.encoding.Base64Encoder)
    page['encrypted_strings'] = box.encrypt(json.dumps(string_vals).encode('utf8'), encoder=nacl.encoding.Base64Encoder).decode('utf8')


def decrypt_page(page, key=settings.REDACTION_KEY, delete_encrypted_strings=False):
    # decrypt stored strings
    box = nacl.secret.SecretBox(key, encoder=nacl.encoding.Base64Encoder)
    strings = json.loads(box.decrypt(page['encrypted_strings'].encode('utf8'), encoder=nacl.encoding.Base64Encoder).decode('utf8'))

    # replace tokens like ['enc', {'i': 1}] with ith entry from strings array
    for block in page['blocks']:
        if block.get('format') == 'image':
            if block.get('redacted', False):
                block['data'] = strings[block['data'][1]['i']]
        else:
            tokens = block.get('tokens', [])
            for i in range(len(tokens)):
                token = tokens[i]
                if type(token) != str and token[0] == 'enc':
                    tokens[i:i + 1] = [strings[token[1]['i']]]

    if delete_encrypted_strings:
        del page['encrypted_strings']


import sys

def info(type, value, tb):
    if hasattr(sys, 'ps1') or not sys.stderr.isatty():
    # we are in interactive mode or we don't have a tty-like
    # device, so we call the default hook
        sys.__excepthook__(type, value, tb)
    else:
        import traceback, pdb
        # we are NOT in interactive mode, print the exception...
        traceback.print_exception(type, value, tb)
        print
        # ...then start the debugger in post-mortem mode.
        # pdb.pm() # deprecated
        pdb.post_mortem(tb) # more "modern"

sys.excepthook = info

