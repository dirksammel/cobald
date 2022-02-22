import pytest

from ..mock.pool import FullMockPool

from cobald.decorator.stopper import Stopper

class TestStopper(object):

    def test_init_enforcement(self):
        pool = FullMockPool()
        with pytest.raises(ValueError):
            Stopper(pool, partition='test', granularity=-10)
        with pytest.raises(ValueError):
            Stopper(pool, partition='test', interval=-10)
        with pytest.raises(ValueError):
            Stopper(pool, partition='')
        with pytest.raises(ValueError):
            Stopper(pool)

            
    def test_running(self):
        pool = FullMockPool()
        stopper = Stopper(pool, partition='test')
        
        for pend_jobs in (2, 7, 150, 5000):
            stopper.n_pend_jobs = pend_jobs
            for value in (0, 1, 5, 10, 1000):
                assert stopper._condition_slurm(value) == value

    def test_idle(self):
        pool = FullMockPool()
        stopper = Stopper(pool, partition='test')
        
        stopper.n_pend_jobs = 0

        for value in (0, 1, 5, 10, 1000):
            assert stopper._condition_slurm(value) == 0

