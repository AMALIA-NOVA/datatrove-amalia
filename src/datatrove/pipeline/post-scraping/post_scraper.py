from typing import Callable, Literal

from datatrove.io import DataFileLike, DataFolderLike
from datatrove.pipeline.readers.base import BaseDiskReader
from datatrove.utils.logging import logger

from datatrove.tools.post_scraping import remove_short_lines, remove_dup_lines


class PostScraper(BaseDiskReader):
    """Applies post-scraping procedures for cleaning up the read text data.

    Args:
        text_key: the key containing the text data (default: "text").
        id_key: the key containing the id for each sample (default: "id").
        remove_short_lines: whether to remove lines shorter than 10 characters (default: True).
        remove_dup_lines: whether to remove duplicated lines (default: True).
    """

    name = "🧹 - POST-SCRAPER"
    _requires_dependencies = ["orjson"]

    def __init__(
        self,
        text_key: str = "text",
        id_key: str = "id",
        remove_short_lines: bool = True,
        remove_dup_lines: bool = True,
    ):
        super().__init__(
            text_key,
            id_key
        )
        self.remove_dup_lines = remove_dup_lines
        self.remove_short_lines = remove_short_lines

    def post_scrape(self, text):
        """Post-scrape the text data."""
        if self.remove_short_lines:
            text = remove_short_lines(text)

        if self.remove_dup_lines:
            text = remove_dup_lines(text)

        return text

    def remove_short_lines(self, text, min_length=10):
        """Remove lines shorter than min_length characters from a text"""
        if not text:
            return text

        # Split text into lines and filter out short ones
        lines = text.splitlines()
        filtered_lines = [line for line in lines if len(line.strip()) > min_length]

        # Rejoin the filtered lines
        return '\n'.join(filtered_lines)

    def remove_dup_lines(self, text):
        """Remove duplicated lines from a text"""
        if not text:
            return text

        # Split text into lines and filter out short ones
        lines = text.splitlines()
        filtered_lines = []
        seen_lines = set()
        for line in lines:
            line = line.strip()
            if line not in seen_lines:
                seen_lines.add(line)
                filtered_lines.append(line)

        # Rejoin the filtered lines
        return '\n'.join(filtered_lines)
