

class Config(object):
    def __init__(self):
        self.CONNECTIONS = {
            'vizio':{
                'host': 'localhost',
                'port': 3306,
                'user': 'benhong',
                'password': '',
                'database': 'vizio'
            }
        }

        self.S3_CONNECTIONS = {
            'vizio': {
                'access_key': "",
                'secret_key': "",
                'bucket': ''
            }
        }
