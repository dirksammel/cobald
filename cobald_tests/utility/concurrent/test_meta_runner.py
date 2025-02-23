import threading
import pytest
import time
import asyncio

import trio

from cobald.daemon.runners.base_runner import OrphanedReturn
from cobald.daemon.runners.meta_runner import MetaRunner


class TerminateRunner(Exception):
    pass


def run_in_thread(payload, name, daemon=True):
    thread = threading.Thread(target=payload, name=name, daemon=daemon)
    thread.start()
    time.sleep(0.0)


class TestMetaRunner(object):
    def test_bool_payloads(self):
        def subroutine():
            time.sleep(0.5)

        async def a_coroutine():
            await asyncio.sleep(0.5)

        async def t_coroutine():
            await trio.sleep(0.5)

        for flavour, payload in (
            (threading, subroutine),
            (asyncio, a_coroutine),
            (trio, t_coroutine),
        ):
            runner = MetaRunner()
            assert not bool(runner)
            runner.register_payload(payload, flavour=flavour)
            assert bool(runner)
            run_in_thread(runner.run, name="test_bool_payloads %s" % flavour)
            assert bool(runner)
            runner.stop()

    @pytest.mark.parametrize("flavour", (threading,))
    def test_run_subroutine(self, flavour):
        """Test executing a subroutine"""

        def with_return():
            return "expected return value"

        def with_raise():
            raise KeyError("expected exception")

        runner = MetaRunner()
        result = runner.run_payload(with_return, flavour=flavour)
        assert result == with_return()
        with pytest.raises(KeyError):
            runner.run_payload(with_raise, flavour=flavour)

    @pytest.mark.parametrize("flavour", (asyncio, trio))
    def test_run_coroutine(self, flavour):
        """Test executing a coroutine"""

        async def with_return():
            return "expected return value"

        async def with_raise():
            raise KeyError("expected exception")

        runner = MetaRunner()
        run_in_thread(runner.run, name="test_run_coroutine %s" % flavour)
        result = runner.run_payload(with_return, flavour=flavour)
        assert result == trio.run(with_return)
        with pytest.raises(KeyError):
            runner.run_payload(with_raise, flavour=flavour)
        runner.stop()

    @pytest.mark.parametrize("flavour", (threading,))
    def test_return_subroutine(self, flavour):
        """Test that returning from subroutines aborts runners"""

        def with_return():
            return "unhandled return value"

        runner = MetaRunner()
        runner.register_payload(with_return, flavour=flavour)
        with pytest.raises(RuntimeError) as exc:
            runner.run()
        assert isinstance(exc.value.__cause__, OrphanedReturn)

    @pytest.mark.parametrize("flavour", (asyncio, trio))
    def test_return_coroutine(self, flavour):
        """Test that returning from subroutines aborts runners"""

        async def with_return():
            return "unhandled return value"

        runner = MetaRunner()
        runner.register_payload(with_return, flavour=flavour)
        with pytest.raises(RuntimeError) as exc:
            runner.run()
        assert isinstance(exc.value.__cause__, OrphanedReturn)

    @pytest.mark.parametrize("flavour", (threading,))
    def test_abort_subroutine(self, flavour):
        """Test that failing subroutines abort runners"""

        def abort():
            raise TerminateRunner

        runner = MetaRunner()
        runner.register_payload(abort, flavour=flavour)
        with pytest.raises(RuntimeError) as exc:
            runner.run()
        assert isinstance(exc.value.__cause__, TerminateRunner)

        def noop():
            return

        def loop():
            while True:
                time.sleep(0)

        runner = MetaRunner()
        runner.register_payload(noop, loop, flavour=flavour)
        runner.register_payload(abort, flavour=flavour)
        with pytest.raises(RuntimeError) as exc:
            runner.run()
        assert isinstance(exc.value.__cause__, TerminateRunner)

    @pytest.mark.parametrize("flavour", (asyncio, trio))
    def test_abort_coroutine(self, flavour):
        """Test that failing coroutines abort runners"""

        async def abort():
            raise TerminateRunner

        runner = MetaRunner()
        runner.register_payload(abort, flavour=flavour)
        with pytest.raises(RuntimeError) as exc:
            runner.run()
        assert isinstance(exc.value.__cause__, TerminateRunner)

        async def noop():
            return

        async def loop():
            while True:
                await flavour.sleep(0)

        runner = MetaRunner()

        runner.register_payload(noop, loop, flavour=flavour)
        runner.register_payload(abort, flavour=flavour)
        with pytest.raises(RuntimeError) as exc:
            runner.run()
        assert isinstance(exc.value.__cause__, TerminateRunner)
