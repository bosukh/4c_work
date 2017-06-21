from config import Config
import os
import re
from datetime import datetime, date
from Queue import Queue
from boto.s3.connection import S3Connection
from local_logger import LocalLogger

logger = LocalLogger(
            logger_name = __name__,
            logfile = 'vizio_file_download_{0}.log'.format(
                datetime.today().strftime(LocalLogger.date_suffix_fmt)
            )
        ).logger


class VizioFileDownloader(object):

    def __init__(self, DBconnection,  year, month, day,):
        # Connection need to be VizioDBConnection
        logger.info('Initializing...')
        self.db_conn = DBconnection
        self.date_str = date(year, month, day).strftime('%Y-%m-%d')


        self.config  = Config()
        access_key = self.config.S3_CONNECTIONS['vizio']['access_key']
        secret_key = self.config.S3_CONNECTIONS['vizio']['secret_key']
        bucket_name = self.config.S3_CONNECTIONS['vizio']['bucket']
        self.s3_conn = S3Connection(access_key, secret_key)
        self.bucket = self.s3_conn.get_bucket(bucket_name)

        filenames     = []
        files_by_date = {}
        no_date_files = []
        for key in self.bucket.list(prefix='vizio/content/content/%s/'%self.date_str):
            name = key.name
            filenames.append(name)
            match = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", name)
            if match:
                date_str = name[match.start():match.end()]
                if files_by_date.get(date_str):
                    files_by_date[date_str].append(key)
                else:
                    files_by_date[date_str] = [key]
            else:
                no_date_files.append(name)

        self.filenames     = filenames
        self.files_by_date = files_by_date
        self.no_date_files = no_date_files
        self.downloaded    = Queue()
        logger.info('Initialization Complete')

    def refresh(self):
        self.__init__(self.db_conn)

    def download(self, path = None, unzip = True, refresh=False):
        # date_str has to be in YYYY-MM-DD
        # For now, download everything again even if there's something.
        if refresh is True:
            self.refresh()

        self.db_conn.load_fileinfo()
        current_files = self.db_conn.fileinfo
        current_file_names = set(current_files.file_name.unique())

        if path is None:
            path = '/files2/Vizio/data/s3_download/vizio_unzipped'

        if not os.path.isdir(os.path.join(path, self.date_str[:-3])):
            os.mkdir(os.path.join(path, self.date_str[:-3]))

        file_path = os.path.join(path, self.date_str[:-3], self.date_str)
        if not os.path.isdir(file_path):
            os.mkdir(file_path)

        for key in self.files_by_date[self.date_str][:10]:
            print 'Downloading file: ', key.name
            _, file_name = os.path.split(key.name)
            dest_file_path = os.path.join(file_path, file_name)
            logger.info(
                'Dowloading file {file_name} to {file_path}'.format(
                    file_name = file_name,
                    file_path = file_path
                )
            )
            try:
                os.remove(dest_file_path[:-3])
                os.remove(dest_file_path)
            except Exception as e:
                pass
            key.get_contents_to_filename(dest_file_path)
            self.db_conn.update_fileinfo(os.path.splitext(file_name)[0],
                                         downloaded_date = datetime.now())

            if unzip is True:
                os.system('gunzip ' + dest_file_path)

        self.file_path = file_path
        return file_path
