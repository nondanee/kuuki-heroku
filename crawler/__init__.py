from . import capture

def run(connect):
    capture.pull_raw_data(connect)
    capture.compact_table(connect)