import pytest

from ..mock.pool import FullMockPool

from cobald.decorator.stopper import Stopper

from unittest.mock import patch

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

    def test_idle(self):
        pool = FullMockPool()
        stopper = Stopper(pool, partition='test')

        with patch('cobald.decorator.stopper.Stopper._check_interval', return_value = False) as check_interval_function:
            stopper.is_stopped = True
            assert stopper._condition_slurm(20) == 0
            stopper.is_stopped = False
            assert stopper._condition_slurm(20) == 20

    def test_check_with_pending(self):
        pool = FullMockPool()
        stopper = Stopper(pool, partition='test')
        
        with patch('cobald.decorator.stopper.Stopper._check_interval', return_value = True) as check_interval_function:
            with patch('cobald.decorator.stopper.Stopper._check_slurm', return_value = '5') as check_slurm_function:
                for value in (0, 1, 5, 10, 1000):
                    assert stopper._condition_slurm(value) == value
                    assert stopper.is_stopped == False        

    def test_check_without_pending(self):
        pool = FullMockPool()
        stopper = Stopper(pool, partition='test')

        with patch('cobald.decorator.stopper.Stopper._check_interval', return_value = True) as check_interval_function:
            with patch('cobald.decorator.stopper.Stopper._check_slurm', return_value = '0') as check_slurm_function:
                for value in (0, 1, 5, 10, 1000):
                    assert stopper._condition_slurm(value) == 0
                    assert stopper.is_stopped == True
