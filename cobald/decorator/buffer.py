import asyncio

from cobald.interfaces import Pool, PoolDecorator

from cobald.daemon import runner


class Buffer(PoolDecorator):
    """
    A timed buffer for changes to a pool

    :param target: the pool to which changes are applied
    :param window: interval after which changes are applied

    Any changes made to :py:attr:`demand` are stored internally.
    Every ``window`` seconds, the final demand is applied to ``target``.
    """
    demand = 0.0

    def __init__(self, target: Pool, window: float = 10.):
        super().__init__(target=target)
        self.window = window
        self.demand = target.demand
        runner.register_subroutine(self.run)

    @asyncio.coroutine
    def run(self):
        while True:
            if self.demand != self.target.demand:
                self.target.demand = self.demand
            yield from asyncio.sleep(self.window)
