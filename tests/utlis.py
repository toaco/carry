import contextlib
import os
import shutil


@contextlib.contextmanager
def chdir(path):
    """A context manager that performs actions in the given directory."""
    orig_cwd = os.getcwd()
    if os.path.isdir(path):
        dir_ = path
    else:
        dir_ = os.path.dirname(path)
    os.chdir(dir_)
    try:
        yield
    finally:
        os.chdir(orig_cwd)


@contextlib.contextmanager
def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)
    yield
    shutil.rmtree(path)
