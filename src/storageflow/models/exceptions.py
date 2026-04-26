class JobExecutionFailed(Exception):
    """Exception thrown when a job execution has failed"""

    def __init__(self, run_uuid: str):
        super().__init__(f"Job with ID {run_uuid} failed.")
        self.run_uuid: str = run_uuid
