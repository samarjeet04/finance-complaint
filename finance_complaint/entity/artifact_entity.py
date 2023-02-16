from collections import namedtuple
from datetime import datetime

DataIngestionArtifact = namedtuple("DataIngestionArtifact",
                                   ["feeature_store_file_path", "metadata_file_path", "download_dir"]

)