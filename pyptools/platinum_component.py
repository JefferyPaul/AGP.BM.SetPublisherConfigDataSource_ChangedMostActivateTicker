import os


class PlatinumStructure:
    def __init__(self, path):
        assert os.path.isdir(path)
        self._path = os.path.abspath(path)


class PlatinumFile:
    def __init__(self, path):
        assert os.path.isfile(path)
        self._path = os.path.abspath(path)
