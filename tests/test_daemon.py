# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

# SPDX-License-Identifier: 	GPL-3.0-or-later

"""
Regression test for the daemon.main() / Twisted task.react() bootstrap.

This guards against two related bugs that both stemmed from the same root
cause: task.react() originally drove _main() via
Deferred.fromCoroutine(coro), which steps the coroutine through Twisted's
own inlineCallbacks-style generator-driving machinery instead of asyncio's
Task machinery.

1. task.react() calls its main callable *before* reactor.run() starts
   pumping the event loop, so anything using asyncio.get_running_loop()
   (e.g. asyncio.to_thread(), used throughout this codebase for blocking
   I/O) would raise "RuntimeError: no running event loop" if it ran too
   soon.
2. Even once the loop was running, awaiting a *second* (or later) real
   asyncio Future/Task from deep inside a Deferred.fromCoroutine()-driven
   coroutine chain could raise a bare
   "RuntimeError: await wasn't used with future" -- Twisted's
   inlineCallbacks stepping does not reliably support this, unlike a
   genuine asyncio.Task. See scheduling.py's PeriodicTask docstring for the
   asyncio.sleep() half of this same class of bug.

daemon.main() now schedules _main() via asyncio.ensure_future() and bridges
*that* real asyncio.Task to a Deferred with Deferred.fromFuture(), which
fixes both: nothing in _main() runs until the loop is genuinely pumping
(asyncio.ensure_future() only schedules a loop.call_soon()), and every
await from then on is driven by asyncio's own Task machinery.

This has to run in a subprocess: elnbuildsync's package import installs a
process-global Twisted reactor as a side effect, and daemon.main() ->
task.react() calls sys.exit() unconditionally when done, which would kill
the pytest process if invoked in-process.
"""

import subprocess
import sys
import textwrap

_SCRIPT = textwrap.dedent(
    """
    import asyncio
    from unittest.mock import patch

    import elnbuildsync.daemon as daemon

    async def _fake_main(*args, **kwargs):
        # This is exactly the shape of the real bug: config.load_static_config()
        # and config.load_dynamic_config() each independently call
        # utils.load_yaml_file() -> asyncio.to_thread(...), so _main() makes
        # *multiple, sequential* real-asyncio-Future awaits. The first
        # exercises the "no running event loop" bug (task.react() drives its
        # callable before reactor.run() starts pumping); the second/third
        # exercise the "await wasn't used with future" bug (Twisted's
        # inlineCallbacks-style coroutine stepping doesn't reliably support
        # awaiting more than one real asyncio Future from the same driven
        # coroutine chain).
        asyncio.get_running_loop()
        one = await asyncio.to_thread(lambda: "one")
        two = await asyncio.to_thread(lambda: "two")
        three = await asyncio.to_thread(lambda: "three")
        print("RESULT:" + "-".join([one, two, three]))

    with patch.object(daemon, "_main", _fake_main):
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            daemon.main,
            [
                "--static-config-file", "{static_config}",
                "--dynamic-config-file", "{static_config}",
                "--db-pw-file", "{db_pw_file}",
            ],
            catch_exceptions=False,
        )
        print("EXIT_CODE:" + str(result.exit_code))
        # click.testing.CliRunner captures stdout for the duration of
        # invoke(), so anything _fake_main() printed only shows up here.
        print(result.output)
    """
)


def test_main_waits_for_reactor_before_running_main(tmp_path):
    static_config = tmp_path / "static.yaml"
    static_config.write_text("")
    db_pw_file = tmp_path / "db_pw"
    db_pw_file.write_text("")

    script = _SCRIPT.format(static_config=static_config, db_pw_file=db_pw_file)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert "RuntimeError: no running event loop" not in proc.stderr, proc.stderr
    assert "RuntimeError: await wasn't used with future" not in proc.stderr, proc.stderr
    assert "RESULT:one-two-three" in proc.stdout, proc.stdout
    assert "EXIT_CODE:0" in proc.stdout, proc.stdout
    assert proc.returncode == 0, proc.stderr
