import json
from datetime import datetime
from hashlib import md5
from pathlib import Path


class JSONSerializer:
    def extension(self, pth: Path):
        return pth.with_suffix(".json")

    def load(self, file):
        return json.load(fp=file)

    def save(self, file, data):
        json.dump(fp=file, obj=data)

    def __repr__(self):
        return f"{self.__class__.__qualname__}()"


class TorchSerializer:
    def __init__(self):
        import torch

        self.torch = torch

    def extension(self, pth: Path):
        return pth.with_suffix(".pt")

    def load(self, file):
        return self.torch.load(file)

    def save(self, file, data):
        self.torch.save(data, file)

    def __repr__(self):
        return f"{self.__class__.__qualname__}()"


class State:
    def __init__(self):
        self.values = {}
        self._directory = None
        self._loaded = False
        self._metadata = None
        self.configure(
            key=None,
            serializer=JSONSerializer(),
            retain=None,
            basedir=".",
        )

    @property
    def _serial(self):
        return self._metadata["serial"]

    @_serial.setter
    def _serial(self, value):
        self._metadata["serial"] = value

    @property
    def _history(self):
        return self._metadata["history"]

    def configure(self, key=None, serializer=None, retain=None, basedir=None):
        if self._loaded:
            raise Exception("Cannot configure the state object after loading.")
        if basedir is not None:
            self.basedir = Path(basedir)
        # if retain is not None:
        #     self.retain = retain
        if serializer is not None:
            self.serializer = serializer
        if key is not None:
            self.key = key
            self.keyhash = md5(json.dumps(key).encode("utf8")).hexdigest()
            self.directory = self.basedir / self.keyhash
            self._latest = self.serializer.extension(self.directory / "latest")

    def snapshot_file(self):
        now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        return self.serializer.extension(
            self.directory / datetime.now().strftime(f"{self._serial:09d}_{now}")
        )

    def __getitem___(self, item):
        return self.values[item]

    def __setitem__(self, item, value):
        self.values[item] = value

    def load_or_init(self, item, default):
        self.load()
        if item not in self.values:
            self.values[item] = default
        return self.values[item]

    def load(self, force=False):
        self.directory.mkdir(parents=True, exist_ok=True)
        if force or not self._loaded:
            self._loaded = True
            if self._latest.exists():
                self.values = self.serializer.load(self._latest.open())
            else:
                self.values = {}
        return self.values

    def load_metadata(self):
        if self._metadata is not None:
            return
        metafile = self.directory / "metadata.json"
        if metafile.exists():
            self._metadata = json.load(metafile.open())
            self._metadata["num_runs"] += 1
        else:
            self._metadata = {
                "serial": 0,
                "num_runs": 1,
                "keyhash": self.keyhash,
                "key": self.key,
                "history": [],
            }
            self.save_metadata()

    def save_metadata(self):
        metafile = self.directory / "metadata.json"
        json.dump(fp=metafile.open("w"), obj=self._metadata, indent=4)

    def save(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.load_metadata()
        self._latest.unlink(missing_ok=True)
        snapshot_file = self.snapshot_file()
        self.serializer.save(snapshot_file.open("w"), self.values)
        self._latest.symlink_to(snapshot_file.name)
        self._history.append(
            {
                "serial": self._serial,
                "run": self._metadata["num_runs"],
                "filename": str(snapshot_file.name),
                "time": str(datetime.now()),
            }
        )
        self._serial += 1
        self.save_metadata()
