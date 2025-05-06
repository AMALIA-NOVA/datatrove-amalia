from datatrove.io import DataFileLike, DataFolderLike
from datatrove.pipeline.post_scraping.base import BasePostScraper
from datatrove.utils.logging import logger


class PostScraper(BasePostScraper):
    """Applies post-scraping procedures for cleaning up the read text data.

    Args:
        text_key: the key containing the text data (default: "text").
        id_key: the key containing the id for each sample (default: "id").
        remove_short_lines: whether to remove lines shorter than 10 characters (default: True).
        remove_dup_lines: whether to remove duplicated lines (default: True).
    """

    name = "🧹 POST-SCRAPER"
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
            text = self.short_lines_remover(text)

        if self.remove_dup_lines:
            text = self.dup_lines_remover(text)

        return text

    def short_lines_remover(self, text, min_length=10):
        """Remove lines with less than min_length alphabetic characters from a text"""
        if not text:
            return text

        lines = text.splitlines()
        # filtered_lines = [line for line in lines if len(line.strip()) > min_length]

        filtered_lines = []
        for line in lines:
            line = line.strip()
            # Check if the line has enough alphabetic characters
            if len([char for char in line if char.isalpha()]) >= min_length:
                filtered_lines.append(line)

        # Rejoin the filtered lines
        return '\n'.join(filtered_lines)

    def dup_lines_remover(self, text):
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
