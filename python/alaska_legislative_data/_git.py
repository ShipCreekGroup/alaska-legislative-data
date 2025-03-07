import contextlib
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


def push_directory_to_github_branch(
    directory: str | Path,
    branch: str,
    *,
    remote: str | None = None,
    commit_message: str = "Update data",
    tmp_git_dir: str | Path = None,
):
    """Overwrites the given branch to have one commit containing the given directory.

    This is useful as a method for publishing data files.
    """
    if tmp_git_dir is None:
        with tempfile.TemporaryDirectory() as tmp_git_dir:
            return push_directory_to_github_branch(
                directory, branch, remote=remote, tmp_git_dir=tmp_git_dir
            )
    tmp_git_dir = Path(tmp_git_dir)
    tmp_git_dir = tmp_git_dir.resolve()
    if tmp_git_dir.exists() and not tmp_git_dir.is_dir():
        raise NotADirectoryError(tmp_git_dir)

    d = Path(directory)
    d = d.resolve()
    if not d.is_dir():
        raise NotADirectoryError(d)

    if remote is None:
        remote = _get_remote()
    tmp_git_dir.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(tmp_git_dir)
    tmp_git_dir.mkdir(parents=True, exist_ok=True)
    with _in_dir(tmp_git_dir):
        shutil.copytree(src=d, dst=tmp_git_dir, dirs_exist_ok=True)
        _run_command("git init")
        _run_command(
            f"git remote -v | grep -w origin || git remote add origin '{remote}'"
        )
        _run_command(f"git checkout -b {branch}")
        _run_command("git add .")
        safe_commit_message = commit_message.replace('"', '\\"')
        _run_command(f'git commit -m "{safe_commit_message}"')
        # Allow for large files? IDK if this is needed, I was running into this
        # error when using my bad hotspot connection.
        # https://stackoverflow.com/questions/59282476/error-rpc-failed-curl-92-http-2-stream-0-was-not-closed-cleanly-protocol-erro
        _run_command("git config http.postBuffer 524288000")
        _run_command(f"git push --set-upstream origin --force {branch}")


@contextlib.contextmanager
def _in_dir(d):
    orig = os.getcwd()
    try:
        os.chdir(d)
        yield
    except:
        raise
    finally:
        os.chdir(orig)


def _get_remote() -> str:
    return _run_command("git config --get remote.origin.url")


def _run_command(command, cwd=None, env=None):
    logger.info(f"Running command: {command}")
    return subprocess.check_output(
        command, cwd=cwd, shell=True, text=True, env=None
    ).strip()


if __name__ == "__main__":
    push_directory_to_github_branch(sys.argv[1], sys.argv[2])
