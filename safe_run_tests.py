# Standard Library Imports
from os import scandir, remove
from shutil import rmtree
from subprocess import Popen, run
from sys import argv


def getTestFilenames(test_dir: str = "tests"):
    for dir_entry in scandir(test_dir):
        if dir_entry.is_file():
            if dir_entry.name.startswith("test_"):
                yield dir_entry.path


def getTestNames(test_file_name: str) -> list:
    tests = []
    with open(test_file_name, 'r') as test_file:
        for line in test_file.readlines():
            if line.startswith("def test_"):
                split_space = line.split()
                test_name = split_space[1]
                test_name = test_name.split("(")[0]

                full_test_name = f"{test_file_name}::{test_name}"
                tests.append(full_test_name)
    return tests


PYTEST_CMD = "pytest"
DEFAULT_OPTIONS = "-s"


def testCommand(test: str) -> list:
    cmd = [PYTEST_CMD]
    if len(argv) > 1:
        cmd.extend(argv[1:])
    else:
        cmd.append(DEFAULT_OPTIONS)
    cmd.append(test)
    return cmd


def cleanDotStrmbrkr():
    """Get rid of '.strmbrkr' files and directories as well as key value store dumps."""
    rm_dirs = []
    rm_files = []
    for dir_entry in scandir():
        if dir_entry.name.startswith(".strmbrkr"):
            if dir_entry.is_dir():
                rm_dirs.append(dir_entry.path)
            elif dir_entry.is_file():
                rm_files.append(dir_entry.path)
        if dir_entry.name.startswith("kvs_dump_"):
            rm_files.append(dir_entry.path)
    for rm_dir in rm_dirs:
        rmtree(rm_dir)
    for rm_file in rm_files:
        remove(rm_file)
    print(f"Removed: {rm_dirs}, {rm_files}")


def runTests(tests: list):
    for test in tests:
        print(f"Running {test}...")
        run(testCommand(test), check=True)
        cleanDotStrmbrkr()


if __name__ == "__main__":
    cleanDotStrmbrkr()
    for test_file_name in getTestFilenames():
        runTests(getTestNames(test_file_name))

    # test_multipleWorkerManagers
    print("Running 'test_multipleWorkerManagers' test...")
    api_instance_0 = Popen(["python", "example.py"])
    api_instance_1 = Popen(["python", "example.py"])

    api_instance_0.wait()
    api_instance_1.wait()
    assert api_instance_0.returncode + api_instance_1.returncode == 0
    cleanDotStrmbrkr()
