import os
import sys
from datetime import datetime, date
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
    date_str = date(year, month, day).strftime('%Y-%m-%d')
    logger.info('Running the script for %s'%date_str)
    importer = VizioImporter(year, month, day)
    downloader = VizioFileDownloader(importer, year, month, day)
    folder_path = downloader.download(path = file_path)
    files = []
    for file_name in os.listdir(folder_path):
        if file_name.find('_manifest') == -1:
            files.append(file_name)
    files.sort()
    for file_name in files:
        current_fileinfo = importer.fileinfo.loc[importer.fileinfo.file_name == file_name]
        if len(current_fileinfo) == 0:
            logger.warning('Table and local directory out of sync. Check %s'%file_name)
            continue
        if current_fileinfo['imported_date'].isnull().sum() > 0:
            importer.import_file(os.path.join(folder_path, file_name))

if __name__ == '__main__':
    args = {}
    for arg in sys.argv[1:]:
        k, v = arg.split('=', 1)
        args[k.strip()] = v.strip()
    date_str = args.get('date')
    file_path = args.get('file_path')
    if date_str is None:
        print "Date is not specified, running for the scrip for today's date"
        today = date.today()
        year, month, day = today.year, today.month, today.day
    else:
        year, month, day = [int(x) for x in date_str.split('-')]
    main(year, month, day, file_path)
