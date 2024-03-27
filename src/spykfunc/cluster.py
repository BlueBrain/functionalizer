"""Module to launch Spark and HDFS clusters on a SLURM allocation."""

import os
import shutil
import socket
import subprocess
import urllib

from pathlib import Path
from mpi4py import MPI

from . import utils


CLUSTER_DATA = Path(__file__).parent / "data" / "cluster"

logger = utils.get_logger(__name__)

_comm = MPI.COMM_WORLD
_rank = _comm.Get_rank()
_size = _comm.Get_size()


def _detect_memory() -> int:
    """Reads ``/proc/meminfo`` and returns free memory in MB."""
    with open("/proc/meminfo") as fd:
        for line in fd:
            if line.startswith("MemFree:"):
                _, mem, _ = line.split()
                return int(mem) // 1024
    raise RuntimeError("Unable to determine memory size")


class Cluster:
    """Wrapper to launch Hadoop and Spark on a SLURM allocation."""

    def __init__(self, workdir: Path):
        """Create a new cluster."""
        self.workdir = workdir
        self.hostname = socket.getfqdn()
        self.rank0 = _comm.bcast(self.hostname, root=0)
        self.env = dict(os.environ)

    def execute(self, fun, *args, **kwargs):
        """Execute a function on the first rank and return the result."""
        result = None
        if _rank == 0:
            old_env = dict(os.environ)
            try:
                os.environ |= self.env
                result = fun(*args, **kwargs)
            finally:
                os.environ = old_env
        _comm.Barrier()
        return result

    @property
    def tmpdir(self):
        """A temporary directory large enough to hold cluster data."""
        if "TMDIR" in self.env:
            return Path(self.env["TMPDIR"])
        username = self.env["USER"]
        jobid = self.env["SLURM_JOBID"]
        return Path("/nvme") / username / jobid

    def __prepare_hadoop_directories(self):
        """Makes sure that all Hadoop directories are ready.

        Returns a dictionary containing environment variables that point to created
        directories.
        """
        procid = ".".join([self.env["SLURM_JOBID"], self.env["SLURM_PROCID"]])
        conf_dir = self.tmpdir / "hadoop" / "conf" / procid
        data_dir = self.tmpdir / "hadoop" / "data" / procid
        tmp_dir = self.tmpdir / "hadoop" / "tmp" / procid

        # Data nodes will fail to start if data from older clusters is still present.
        shutil.rmtree(data_dir, ignore_errors=True)

        log_dir = self.workdir / "hadoop" / "logs"
        name_dir = self.workdir / "hadoop" / "name"

        if _rank == 0:
            log_dir.mkdir(parents=True, exist_ok=True)
            name_dir.mkdir(parents=True, exist_ok=True)

        conf_dir.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        self.env |= {
            "HADOOP_CONF_DIR": str(conf_dir),
            "HADOOP_LOG_DIR": str(log_dir),
        }

        return (conf_dir, data_dir, name_dir, tmp_dir)

    def __prepare_hadoop_environment(self):
        """Returns environment variables for Hadoop based on SLURM allocations."""
        if not (java_home := self.env.get("JAVA_HOME")):
            java_home = (
                Path(subprocess.check_output(["which", "java"]).decode().strip())
                .resolve()
                .parent.parent
            )
        if not (hadoop_home := self.env.get("HADOOP_HOME")):
            hadoop_home = (
                Path(subprocess.check_output(["which", "hdfs"]).decode().strip())
                .resolve()
                .parent.parent
            )

        self.env |= {
            "HADOOP_HOME": str(hadoop_home),
            # Log rotation in Hadoop checks for the environment variable explicitly,
            # BlueBrain5 does not set it, so do it ourselves.
            "HOSTNAME": self.hostname,
            "JAVA_HOME": str(java_home),
        }

        return Path(hadoop_home)

    def __prepare_spark_directories(self):
        """Makes sure that all Spark directories are ready.

        Returns a dictionary containing environment variables that point to created
        directories.
        """
        log_dir = self.workdir / "logs"
        local_dir = self.tmpdir / "spark-local"
        worker_dir = self.tmpdir / "spark-worker"

        if _rank == 0:
            log_dir.mkdir(parents=True, exist_ok=True)
        local_dir.mkdir(parents=True, exist_ok=True)
        worker_dir.mkdir(parents=True, exist_ok=True)

        self.env |= {
            "SPARK_LOG_DIR": str(log_dir),
            "SPARK_LOCAL_DIRS": str(local_dir),
            "SPARK_WORKER_DIR": str(worker_dir),
        }

    def __prepare_spark_environment(self):
        """Returns environment variables for Spark based on SLURM allocations."""
        # Use a provided Python script in case we're relying on PySpark
        if not (spark_home := self.env.get("SPARK_HOME")):
            spark_home = Path(subprocess.check_output(["find_spark_home.py"]).decode().strip())

        master_memory = self.env.get("SM_MASTER_MEMORY", 16384)
        memory_margin = self.env.get("SM_MEMORY_MARGIN", 8192)

        cpus_per_task = self.env.get("SLURM_CPUS_PER_TASK")
        worker_memory = self.env.get("SM_WORKER_MEMORY")
        if not worker_memory:
            worker_memory = self.env.get("SLURM_MEM_PER_NODE")
        if not worker_memory:
            cpu_memory = self.env.get("SLURM_MEM_PER_CPU")
            if cpu_memory and cpus_per_task:
                worker_memory = int(cpu_memory) * int(cpus_per_task)
        detected_memory = _detect_memory() - int(memory_margin)
        if _rank == 0:
            detected_memory -= int(master_memory)
        if not worker_memory:
            worker_memory = detected_memory
        else:
            worker_memory = min(int(worker_memory), detected_memory)

        worker_cores = self.env.get("SM_WORKER_CORES", cpus_per_task)
        if not worker_cores:
            worker_cores = self.env.get("SLURM_CPUS_ON_NODE")

        self.env |= {
            "SPARK_HOME": str(spark_home),
            "SPARK_DAEMON_MEMORY": f"{master_memory}m",
            "SPARK_WORKER_CORES": str(worker_cores),
            "SPARK_WORKER_MEMORY": f"{worker_memory}m",
            # Log rotation in Spark checks for the environment variable explicitly,
            # BlueBrain5 does not set it, so do it ourselves.
            "HOSTNAME": self.hostname,
        }

        # When launching Spark, make sure that any MPI related variables are purged as to
        # not crash the Python workers.
        to_remove = [k for k in self.env if "PMI" in k]
        for k in to_remove:
            del self.env[k]

        return Path(spark_home), worker_cores, worker_memory

    def launch_hadoop(self):
        """Launch a Hadoop cluster on the current allocation."""
        hadoop_home = self.__prepare_hadoop_environment()
        conf_dir, data_dir, name_dir, tmp_dir = self.__prepare_hadoop_directories()
        name_dir = urllib.parse.quote(str(name_dir))

        data = {
            "data_dir": data_dir,
            "master": self.rank0,
            "name_dir": name_dir,
            "tmp_dir": tmp_dir,
        }

        for pth in CLUSTER_DATA.iterdir():
            if pth.is_file():
                content = pth.read_text().format(**data)
                (conf_dir / pth.name).write_text(content)

        hdfs = hadoop_home / "bin" / "hdfs"

        if _rank == 0:
            subprocess.check_call(
                [hdfs, "namenode", "-format", "-force", "-nonInteractive"], env=self.env
            )
            subprocess.check_call(
                [hdfs, "--config", conf_dir, "--daemon", "start", "namenode"],
                env=self.env,
            )
            logger.info("Web UI for Hadoop: http://%s:50070", self.rank0)

        _comm.Barrier()

        subprocess.check_call(
            [hdfs, "--config", conf_dir, "--daemon", "start", "datanode"], env=self.env
        )
        _comm.Barrier()

    def launch_spark(self):
        """Launch a Spark cluster on the current allocation."""
        spark_home, worker_cores, worker_memory = self.__prepare_spark_environment()
        self.__prepare_spark_directories()

        spark_master = spark_home / "sbin" / "start-master.sh"
        spark_worker = spark_home / "sbin" / "start-worker.sh"

        if _rank == 0:
            subprocess.check_call([spark_master], env=self.env)
            # Will be picked up by spykfunc.utils.conf to connect Spark to the right host
            # and default parallelism
            self.env["PYSPARK_MASTER"] = f"spark://{self.rank0}:7077"
            self.env["PYSPARK_PARALLELISM"] = str(int(worker_cores or 2) * _size * 2)
            logger.info("Web UI for Spark: http://%s:8080", self.rank0)

        _comm.Barrier()

        args = [
            spark_worker,
            f"spark://{self.rank0}:7077",
            "-m",
            f"{worker_memory}m",
            "-c",
            str(worker_cores),
        ]
        subprocess.check_call(args, env=self.env)
        _comm.Barrier()
