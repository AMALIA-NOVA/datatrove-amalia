from datatrove.executor import SlurmPipelineExecutor
from datatrove.executor.slurm import launch_slurm_job
from dotenv import load_dotenv
import os


class MareNostrumExecutor(SlurmPipelineExecutor):
    """Custom executor for MareNostrum5 supercomputer.
    This executor is a subclass of SlurmPipelineExecutor and is used to
    configure the Slurm job submission parameters for MareNostrum.
    It overrides the get_sbatch_args and launch_merge_stats methods to
    customize the job submission process and not specify memory; and the
    get_launch_file_contents to add the HF_HOME export."""

    def get_sbatch_args(self, max_array: int = 1) -> dict:
        """Remove memory parameters since MareNostrum allocates memory based on CPU count."""
        sbatch_args = super().get_sbatch_args(max_array)
        # Remove mem-per-cpu from the arguments
        if "mem-per-cpu" in sbatch_args:
            del sbatch_args["mem-per-cpu"]
        return sbatch_args

    def launch_merge_stats(self):
        """Override to remove memory parameters for stats job."""
        merge_args = {
            **self.get_sbatch_args(),
            "cpus-per-task": 1,
            # No mem-per-cpu parameter
            "dependency": f"afterok:{self.job_id}",
        }

        launch_slurm_job(
            self.get_launch_file_contents(
                merge_args,
                f'merge_stats {self.logging_dir.resolve_paths("stats")} '
                f'-o {self.logging_dir.resolve_paths("stats.json")}'
            ),
            self.job_id_retriever
        )

    def get_launch_file_contents(self, sbatch_args, run_script):
        """Add HF_HOME export to launch script."""
        content = super().get_launch_file_contents(sbatch_args, run_script)

        load_dotenv()  # Load environment variables from .env file if it exists

        hf_base = os.environ.get("HF_HOME")
        if hf_base is None:
            raise ValueError(
                "HF_HOME is not set. Add it to your .env file or export it in your shell."
            )
        extra_pythonpath = os.environ.get("EXTRA_PYTHONPATH", "")

        env_exports = [
            f'export HF_HOME="{hf_base}"',
            f'export TRANSFORMERS_CACHE="{hf_base}/hub"',
            f'export HF_ASSETS_CACHE="{hf_base}/assets"',
            f'export HF_HUB_CACHE="{hf_base}/hub"',
        ]
        if extra_pythonpath:
            env_exports.append(f'export PYTHONPATH="{extra_pythonpath}:$PYTHONPATH"')

        lines = content.split('\n')
        for i, line in enumerate(lines):
            if "export PYTHONUNBUFFERED=TRUE" in line:
                for j, export_line in enumerate(env_exports):
                    lines.insert(i + 1 + j, export_line)
                break

        return '\n'.join(lines)
