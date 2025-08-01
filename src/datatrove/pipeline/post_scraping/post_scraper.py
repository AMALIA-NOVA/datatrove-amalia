from datatrove.io import DataFileLike, DataFolderLike
from datatrove.pipeline.post_scraping.base import BasePostScraper
from datatrove.utils.logging import logger
from datatrove.pipeline.writers.disk_base import DiskWriter
from bs4 import BeautifulSoup


class PostScraper(BasePostScraper):
    """Applies post-scraping procedures for cleaning up the read text data.

    Args:
        text_key: the key containing the text data (default: "text").
        id_key: the key containing the id for each sample (default: "id").
        remove_short_lines: whether to remove lines shorter than 10 characters (default: True).
        remove_dup_lines: whether to remove duplicated lines (default: True).
        remove_no_punct_lines: whether to remove lines that do not end with a punctuation mark (default: True).
        remove_html_tags: whether to remove HTML tags from the text (default: True).
    """

    name = "🧹 POST-SCRAPER"
    _requires_dependencies = ["orjson"]

    def __init__(
        self,
        exclusion_writer: DiskWriter = None,
        text_key: str = "text",
        id_key: str = "id",
        remove_short_lines: bool = False,
        remove_dup_lines: bool = False,
        remove_no_punct_lines: bool = False,
        remove_html_tags: bool = False,
    ):
        super().__init__(
            exclusion_writer,
            text_key,
            id_key
        )
        self.remove_dup_lines = remove_dup_lines
        self.remove_short_lines = remove_short_lines
        self.remove_no_punct_lines = remove_no_punct_lines
        self.remove_html_tags = remove_html_tags

    def post_scrape(self, text):
        """Post-scrape the text data."""

        if self.remove_short_lines:
            text = self.short_lines_remover(text)

        if self.remove_dup_lines:
            text = self.dup_lines_remover(text)

        if self.remove_no_punct_lines:
            text = self.no_punctuaction_line_remover(text)

        if self.remove_html_tags:
            text = self.html_tags_remover(text)

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
        return '\n'.join(filtered_lines) if filtered_lines else ""

    def dup_lines_remover(self, text):
        """Remove duplicated lines from a text"""
        if not text:
            return text

        lines = text.splitlines()
        filtered_lines = []
        seen_lines = set()
        for line in lines:
            line = line.strip()
            if line not in seen_lines:
                seen_lines.add(line)
                filtered_lines.append(line)

        # Rejoin the filtered lines
        return '\n'.join(filtered_lines) if filtered_lines else ""

    def no_punctuaction_line_remover(self, text):
        """Remove lines that do not end with a punctuation mark."""
        if not text:
            return text

        lines = text.splitlines()
        filtered_lines = [line for line in lines if line.strip() and line.strip()[-1] in ".,;:!?"]

        return '\n'.join(filtered_lines) if filtered_lines else ""

    def html_tags_remover(self, text):
        """Remove HTML tags from the text."""
        if not text:
            return text

        # Remove HTML tags
        soup = BeautifulSoup(text, 'html.parser')
        clean_text = soup.get_text()

        return clean_text.strip() if clean_text else ""
