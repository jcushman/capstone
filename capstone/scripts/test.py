from pathlib import Path

from capdb.models import CaseMetadata, CaseHTML



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




c = CaseMetadata.objects.get(case_id='32044038687000_0040')
c.sync_from_initial_metadata()
h = CaseHTML(metadata=c)
h.render()
Path('test.html').write_text(h.html)