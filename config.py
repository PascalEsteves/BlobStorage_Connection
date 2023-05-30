from os.path import exists
import codecs
from json import load

class Config(object):

    def __init__(self,
                 cfg_object:object,
                 required_attributes,
                 encoding="utf-8"):

        self._config=cfg_object

        valid,missing=self.val_config(required_attributes)

        if not valid:
            raise ValueError(f"Missing required attribute(s): {','.join(missing)}")

    def get_property(self, property_name, default=None):

        if property_name not in self._config.keys():
            return default
        return self._config[property_name]

    def val_config(self, required_attributes):
        missing = [x for x in required_attributes if self.get_property(x) == None]
        return (len(missing) < 1, missing)

class FileConfig(Config):
    def __init__(self, filepath, required_attributes, encoding="utf8", key=None):
        assert exists(filepath), FileNotFoundError(filepath)
        with codecs.open(filepath, "r", encoding) as jf:
            file_data = load(jf)

        super().__init__(file_data, required_attributes, encoding)
        self.filepath = filepath

class Valid_Blob_Config(FileConfig):

    required_columns=[
    "blob_user",
    "blob_key"
    ]

    @property
    def blob_user(self):
        return self.get_property("blob_user")
    @property
    def blob_key(self):
        return self.get_property("blob_key")
      
    def __init__(self, config_filepath, encoding="utf8"):
        super().__init__(config_filepath, Valid_Blob_Config.required_columns, encoding)
