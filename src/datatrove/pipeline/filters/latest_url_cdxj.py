import os
import orjson
from datatrove.data import Document
from datatrove.io import safely_create_file
from datatrove.utils._import_utils import ASSETS_PATH
from datatrove.utils.logging import logger

from ..writers.disk_base import DiskWriter
from .base_filter import BaseFilter


class LatestURLFilter(BaseFilter):
    """
    Filters URLS so that only unique URLs are kept (leaving the one with the latest date).
    This filter uses de filtered cdxj files to know which URLS to keep.

    Args:
        exclusion_writer: DiskWriter to write the excluded URLs.
        filtered_cdxj_file: Path to the filtered cdxj file with the URLs to keep.
    """

    name = "1️⃣ Latest URL"

    def __init__(
            self,
            exclusion_writer: DiskWriter = None,
            filtered_cdxj_file: str = None,
    ):

        super().__init__(exclusion_writer)
        self.filtered_cdxj_file = filtered_cdxj_file
        self.url_date_map = {}

        if filtered_cdxj_file and os.path.exists(filtered_cdxj_file):
            try:
                with open(filtered_cdxj_file, "r", encoding="utf-8") as f:
                    for line in f:
                        parts = line.split(" ", 2)
                        date_part = parts[1]
                        json_content = orjson.loads(parts[2])
                        url_part = json_content.get("url")

                        # Keep only the latest date for each URL
                        if url_part in self.url_date_map:
                            if date_part > self.url_date_map[url_part]:
                                self.url_date_map[url_part] = date_part
                        else:
                            self.url_date_map[url_part] = date_part


                logger.info(f"Loaded {len(self.url_date_map)} URLs from CDXJ file")
            except Exception as e:
                logger.error(f"Error loading CDXJ file: {e}")
                self.url_date_map = {}

    def filter(self, document: Document) -> bool | tuple[bool, str]:
        url = document.metadata.get("url")
        date = document.metadata.get("date")

        assert url, "Document does not have url in its metadata"
        assert date, "Document does not have date in its metadata"

        if not self.filtered_cdxj_file:
            logger.warning("Filtered cdxj path is not set. Skipping URL filtering.")
            return True
        if not self.url_date_map:
            logger.warning("URL date map is empty. Skipping URL filtering.")
            return True

        if url not in self.url_date_map:
            # URL is not in the filtered cdxj file, so we can keep it
            return False, "url_not_in_cdxj"

        # URL is in the filtered cdxj file, so we need to check the date
        if date < self.url_date_map[url]:
            # The date of the document is older than the latest date in the cdxj file
            return False, "url_older_than_cdxj"

        return True
