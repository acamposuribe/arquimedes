from __future__ import annotations

import plistlib

from arquimedes.launchd import render_plist


def test_render_plist_with_start_interval():
    text = render_plist(
        "com.arquimedes.watch",
        ["python", "-m", "arquimedes.cli", "watch", "--once"],
        working_directory="/repo",
        start_interval=1800,
        run_at_load=True,
    )

    data = plistlib.loads(text.encode("utf-8"))
    assert data["Label"] == "com.arquimedes.watch"
    assert data["StartInterval"] == 1800
    assert data["RunAtLoad"] is True
    assert data["ProgramArguments"][-1] == "--once"
    assert data["EnvironmentVariables"]["PATH"]


def test_render_plist_with_calendar_interval():
    text = render_plist(
        "com.arquimedes.lint-full",
        ["python", "-m", "arquimedes.cli", "lint", "--full"],
        working_directory="/repo",
        start_calendar_interval={"Hour": 2, "Minute": 0},
    )

    data = plistlib.loads(text.encode("utf-8"))
    assert data["StartCalendarInterval"] == {"Hour": 2, "Minute": 0}
    assert data["EnvironmentVariables"]["PATH"]
