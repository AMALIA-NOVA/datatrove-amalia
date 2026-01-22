from datatrove.pipeline.base import PipelineStep
from datatrove.data import DocumentsPipeline
from datatrove.utils.typeshelper import StatHints
from datatrove.pipeline.writers.disk_base import DiskWriter

class KeepHigherQuality(PipelineStep):
    """Keeps the document with the highest quality score from each cluster of duplicates."""

    name = "☀️ KEEP HIGHER QUALITY"

    def __init__(self, exclusion_writer: DiskWriter = None):
        super().__init__()
        self.exclusion_writer = exclusion_writer

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        with self.track_time():
            cluster_best_quality = {}  # Only store quality scores, not full documents

            # First pass: find best quality score per cluster
            docs_buffer = []
            for doc in data:
                docs_buffer.append(doc)
                cluster_id = doc.metadata.get("minhash_cluster_id", -1)

                if cluster_id != -1:
                    quality_score = doc.metadata.get("quality_score", 0)
                    if cluster_id not in cluster_best_quality or quality_score > cluster_best_quality[cluster_id]:
                        cluster_best_quality[cluster_id] = quality_score

            # Track clusters that have already yielded their best document
            cluster_yielded = set()

            # Second pass: yield only best documents (one per cluster)
            for doc in docs_buffer:
                self.stat_update(StatHints.total)
                cluster_id = doc.metadata.get("minhash_cluster_id", -1)
                if cluster_id == -1:
                    # Single document, always keep
                    self.update_doc_stats(doc)
                    self.stat_update(StatHints.forwarded)
                    # print(f"Cluster {cluster_id} - Size {doc.metadata.get('minhash_cluster_size', 0)}: Keeping single document.")
                    yield doc
                elif cluster_id not in cluster_yielded:
                    quality_score = doc.metadata.get("quality_score", 0)
                    if quality_score == cluster_best_quality[cluster_id]:
                        # This is the best document in the cluster
                        cluster_yielded.add(cluster_id)
                        self.update_doc_stats(doc)
                        self.stat_update(StatHints.forwarded)
                        # print(f"Cluster {cluster_id} - Size {doc.metadata.get('minhash_cluster_size', 0)}: Keeping document with quality score {quality_score}.")
                        yield doc
                    else:
                        self.update_doc_stats(doc)
                        self.stat_update(StatHints.dropped)
                        # print(f"Cluster {cluster_id} - Size {doc.metadata.get('minhash_cluster_size', 0)}: Excluding document with quality score {quality_score}.")
                        if self.exclusion_writer:
                            self.exclusion_writer.write(doc, rank)
                else:
                    # Cluster already yielded its best, drop this duplicate
                    self.update_doc_stats(doc)
                    self.stat_update(StatHints.dropped)
                    # quality_score = doc.metadata.get("quality_score", 0)
                    # if quality_score == cluster_best_quality[cluster_id]:
                    #     print(f"Cluster {cluster_id} - Size {doc.metadata.get('minhash_cluster_size', 0)}: Duplicate document with quality score {quality_score}.")
                    # else:
                    #     print(f"Cluster {cluster_id} - Size {doc.metadata.get('minhash_cluster_size', 0)}: Excluding document with quality score {quality_score}.")
                    if self.exclusion_writer:
                        self.exclusion_writer.write(doc, rank)
