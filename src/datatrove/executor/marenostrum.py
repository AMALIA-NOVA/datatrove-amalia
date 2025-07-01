from datatrove.executor import SlurmPipelineExecutor
from datatrove.executor.slurm import launch_slurm_job


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

        # Add the HF_HOME export before the run_script
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if "export PYTHONUNBUFFERED=TRUE" in line:
                # Add our export after this line
                lines.insert(i + 1, 'export HF_HOME="/gpfs/projects/epor32/"')
                lines.insert(i + 2, 'export TRANSFORMERS_CACHE="/gpfs/projects/epor32/hub"')
                lines.insert(i + 3, 'export HF_ASSETS_CACHE="/gpfs/projects/epor32/assets"')
                lines.insert(i + 4, 'export HF_HUB_CACHE="/gpfs/projects/epor32/hub"')
                lines.insert(i + 5, 'export PYTHONPATH="/gpfs/projects/epor32/gvmartins/arquivo-filter:$PYTHONPATH"')
                # lines.insert(i + 3, 'export PATH=$PATH:/gpfs/projects/epor32/gvmartins/datatrove_amalia/src/datatrove/tools')
                break

        return '\n'.join(lines)
