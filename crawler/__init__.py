from . import capture

def run(connect):
    capture.pullRawData(connect)
    # capture.processData(connect)
    capture.compactTable(connect)
