import os
import sys
from datetime import datetime
from local_logger import LocalLogger
from vizio_data_import import VizioImporter
from vizio_file_download import VizioFileDownloader

logger = LocalLogger(
            logger_name = __name__,
            logfile = 'vizio_main_{0}.log'.format(
                datetime.today().strftime(LocalLogger.date_suffix_fmt)
            )
        ).logger

def main(year, month, day, file_path):
    importer = VizioImporter(year, month, day)
    downloader = VizioFileDownloader(importer, year, month, day)
    folder_path = downloader.download(path = file_path)
    files = []
    for file_name in os.listdir(folder_path):
        if file_name.find('_manifest') == -1:
            files.append(file_name)
    files.sort()
    for file_name in files:
        importer.import_file(os.path.join(folder_path, file_name))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'date string "YYYY-MM-DD" is needed'
    else:
        date_str = sys.argv[1]
        file_path = None
        if len(sys.argv) > 2:
            file_path = sys.argv[2]
        year, month, day = [int(x) for x in date_str.split('-')]
        main(year, month, day, file_path)
