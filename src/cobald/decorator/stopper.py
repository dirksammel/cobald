from cobald.interfaces import Pool, PoolDecorator

from ..utility import enforce

import subprocess
import asyncio

from cobald.daemon import service

@service(flavour=asyncio)
class Stopper(PoolDecorator):
    """
    Decorator that sets the demand to 0 if there are no pending jobs on the partition

    :param target: the pool
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
        """Set demand to zero if there are no pending jobs, otherwise pass `value`"""
        if self.n_pend_jobs == 0:
            return 0
        else:
            return value

    async def run(self):
        """Retrieve the number of pending jobs on `partition`"""
        while True:
            proc = subprocess.Popen(f"squeue -p {self.partition} -t pending -h | wc -l",
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            self.n_pend_jobs = int(stdout.decode('ascii').strip())
            await asyncio.sleep(self.interval)

    # async def run(self):
    #     """Retrieve the number of pending jobs on `partition`"""
    #     while True:
    #         proc = await asyncio.create_subprocess_shell(
    #             f"squeue -p {self.partition} -t pending -h | wc -l",
    #             stdout=asyncio.subprocess.PIPE, 
    #             stderr=asyncio.subprocess.PIPE)
    #         stdout, stderr = await proc.communicate()
    #         self.n_pend_jobs = int(stdout.decode('ascii').strip())
    #         await asyncio.sleep(self.interval)

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
        self.interval = interval
        self.n_pend_jobs = 0
