from snakebite.client import Client
from hdfs import InsecureClient


class Client:
    def __init__(self, host='localhost', port=9000, user):
        self.client = Client(host, port, version)
        self.client2 = InsecureClient('http://{}:{}'.format(host, port), user=user)

    def isExist(self, path):
        if len(self.client.cat([path])) return True
        return False

    def copyToLocal(self, src, dst):
        if self.isExist(src):
            return self.client.copyToLocal([src], dst)

    def delete(self, path):
        if self.isExist(src):
            return self.client.delete([path])

    def mkdir(self, path):
        if !self.isExist(src):
            return self.client.mkdir([path], create_parent=True)

    def read(self, file_path):
        with self.client2.read(path) as reader:
            return reader.read()

    # # Writing part of a file.
    # with open('samples') as reader, client.write('samples') as writer:
    # for line in reader:
    # if line.startswith('-'):
    # writer.write(line)
    # # Writing a serialized JSON object.
    # with client.write('model.json', encoding='utf-8') as writer:
    # from json import dump
    # dump(model, writer)
