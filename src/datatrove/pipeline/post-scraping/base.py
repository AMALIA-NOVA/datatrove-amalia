from abc import abstractmethod

from datatrove.pipeline.base import PipelineStep
from datatrove.data import DocumentsPipeline
from datatrove.utils.typeshelper import StatHints
from datatrove.utils.logging import logger


class BasePostScraper(PipelineStep):
    """Base module for PostScrapers. PostScrapers are used to process the scraped data from a source.
        PostScrapers are the last step in a pipeline usually.

    Args:
        text_key: key to use for the text in the default adapter (default: "text").
        id_key: key to use for the id in the default adapter (default: "id").
    """

    type = "🧹 - POST-SCRAPER"

    def __init__(
            self,
            text_key: str = "text",
            id_key: str = "id",
    ):
        super().__init__()
        self.text_key = text_key
        self.id_key = id_key


    @abstractmethod
    def post_scrape(self, text: str) -> str:
        """Post-scrape the text data.

        Args:
            text: str: non-plain text

        Returns: scraped plain text

        """
        pass


    def run(self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """Iterates through each document in data and calls the post scraping functions

        Args:
          data: DocumentsPipeline:
          rank: int:  (Default value = 0)
          world_size: int:  (Default value = 1)

        Returns:

        """
        for doc in data:
            with self.track_time():
                try:
                    doc.text = self.post_scrape(doc.text)
                    self.stat_update("post-scraped")
                except EOFError:
                    # Process died unexpectedly
                    self.stat_update("broken_process")
                    logger.warning("Process died unexpectedly, will create new process for next document")
                    continue
                except Exception as e:
                    self.stat_update("post-scraping_error")
                    logger.warning(
                        f'❌ Error "{e}" while post-scraping record text. Skipping record. '
                        f"This message will only appear once."
                    )
                    continue

            if doc.text:
                self.update_doc_stats(doc)
                yield doc