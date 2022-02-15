from cobald.interfaces import Pool, PoolDecorator

from ..utility import enforce

from datetime import datetime
import subprocess

class Stopper(PoolDecorator):
    """
    Decorator that sets the demand to 0 if there are no pending jobs on the partition

    :param target: the pool on which changes are standardised
    :param granularity: granularity of ``target.demand``
    :param partition: partition that is checked for pending jobs
    :param interval: interval in seconds between checks of ``partition``

    If there are pending jobs on the partition, the demand is not modified. Otherwise, the demand is set to 0 and kept at 0 as long as there no pending jobs are detected.

    The default interval is 300 (5 minutes). The partition has to be specified.
    """

    @property
    def demand(self) -> float:
        if abs(self._demand - self.target.demand) >= self.granularity:
            self._demand = self.target.demand
        return self._demand

    @demand.setter
    def demand(self, value: float):
        # Record demand
        self._demand = self._condition_slurm(value)
        if self.granularity != 1:
            self.target.demand = self._condition_slurm(_floor(value, self.granularity))
        else:
            self.target.demand = self._demand
    
    def _condition_slurm(self, value):
        """Check every `interval` seconds for pending jobs on `partition`, return `value` if there are pending jobs, otherwise return 0"""
        time_delta = (datetime.now() - self.test_time).total_seconds()
        if time_delta >= self.interval:
            output, error = subprocess.Popen(f"squeue -p {self.partition} -t pending -h | wc -l", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            n_pend_jobs = output.decode('ascii').strip()
            self.test_time = datetime.now()
            if n_pend_jobs == '0':
                self.is_stopped = True
                return 0
            else:
                self.is_stopped = False
                return value
        else:
            if self.is_stopped:
                return 0
            else:
                return value

    def __init__(
        self,
        target: Pool,
        granularity: int = 1,
        partition: str = '',
        interval: int = 300,
    ):
        super().__init__(target)
        enforce(granularity > 0, ValueError("granularity must be positive"))
        enforce(interval > 0, ValueError("interval must be positive"))
        enforce(partition != '', ValueError("partition must be specified"))
        self._demand = target.demand
        self.granularity = granularity
        self.partition = partition
        self.test_time = datetime.now()
        self.interval = interval
        self.is_stopped = True
