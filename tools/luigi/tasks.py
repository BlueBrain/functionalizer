from bbp_workflow import slurm
from datetime import timedelta
from luigi.contrib.ssh import RemoteContext
from luigi.util import requires

import itertools
import luigi
import os
import shutil

from spykfunc.tools import scaling as S

HOST = "bbpv2"
CONFENV = "/gpfs/bbp.cscs.ch/project/proj16/matwolf/_spark_{version}/env.sh"
DATADIR = "/gpfs/bbp.cscs.ch/project/proj16/spykfunc/circuits"
CIRCUIT_SPEC = "{builderRecipeAllPathways.xml,circuit.mvd3,morphologies/h5,touches/*.parquet}"


class Spykfunc(luigi.Task):
    basedir = luigi.Parameter(significant=False)
    circuit = luigi.Parameter()
    version = luigi.Parameter(default='2.2.1')
    cores = luigi.Parameter()
    node_cores = luigi.Parameter()
    exec_cores = luigi.Parameter()
    exec_memory = luigi.Parameter(significant=False)

    cleanup = luigi.Parameter(significant=False, default=False)
    queue = luigi.Parameter(significant=False, default='prod')
    hours = luigi.Parameter(significant=False, default=8)

    @staticmethod
    def _format_slurm(values):
        result = []
        for k, v in values.items():
            if isinstance(v, bool):
                if v:
                    result.append("#SBATCH --{}".format(k))
            else:
                result.append("#SBATCH --{}={}".format(k, v))
        return "\n".join(result)

    def _name(self):
        nodes = self.cores // self.node_cores
        execs = self.cores // self.exec_cores
        return "{}/{}cores_{}nodes_{}execs".format(self.circuit, self.cores, nodes, execs)

    def _workdir(self):
        return os.path.join(self.basedir, self._name())

    def run(self):
        if self.cleanup and os.path.exists(self._workdir()):
            shutil.rmtree(self._workdir())
        nodes = self.cores // self.node_cores

        spark_opts = " ".join([
            "--master spark://$(hostname):7077",
            "--driver-memory {mem}G",
            "--executor-cores {cores}",
            "--executor-memory {mem}G"
        ]).format(mem=self.exec_memory,
                  cores=self.exec_cores)

        if not os.path.exists(self._workdir()):
            os.makedirs(self._workdir())

        cmd = "\n".join([
            ". {env}",
            "export DATADIR={dd}",
            "export OUTDIR={od}",
            "export SM_WORKER_CORES=$(({nc} + 1))",
            "export SM_EXECUTE='spykfunc --name {nm} --output-dir=$OUTDIR --spark-opts \"{so}\" {args}'",
            "mkdir -p $OUTDIR",
            "cd $OUTDIR",
            "sm_cluster startup $OUTDIR/_cluster {env}",
            "exit $?"
        ]).format(dd=os.path.join(DATADIR, self.circuit),
                  od=self._workdir(),
                  nm=self._name(),
                  nc=self.node_cores,
                  so=spark_opts,
                  args=os.path.join("$DATADIR", CIRCUIT_SPEC),
                  env=CONFENV.format(version=self.version))

        extras = {
            "exclusive": True,
            "nodes": nodes,
            "mem": 0,
            "constraint": "nvme",
        }

        if nodes > 24:
            extras["qos"] = "bigjob"

        remote = RemoteContext(HOST)
        sl = slurm.Slurm(remote)
        code = sl.launch_job('proj16', self.queue, 'fz_' + self._name(), timedelta(hours=self.hours), cmd,
                             self._workdir(),
                             self._format_slurm(extras)) \
                 .wait()
        if not slurm.SlurmExecState.issuccessful(code):
            return

        fn = os.path.join(self._workdir(), "report.json")

        with self.output().open('w') as out:
            with open(fn) as fd:
                out.write(fd.read())
        if self.cleanup:
            shutil.rmtree(self._workdir())

    def output(self):
        return luigi.LocalTarget(self._workdir() + ".json")


@requires(Spykfunc)
class SpykfuncGanglia(luigi.Task):
    def run(self):
        list(S.extract_data([self.input().fn], timeline=True))

    def output(self):
        return luigi.LocalTarget(self.input().fn.replace(".json", ".pkl"))


class Scaling(luigi.WrapperTask):
    basedir = luigi.Parameter(significant=False)
    executors = luigi.ListParameter(default=[(9, 30), (18, 70)])
    nodes = luigi.ListParameter(default=[8, 16, 24])
    cores = luigi.ListParameter(default=[36])
    circuits = luigi.ListParameter(default="O1.v6a S1.v6a 10x10".split())
    version = luigi.Parameter(default='2.2.1')

    cleanup = luigi.BoolParameter(significant=False, default=False)
    queue = luigi.Parameter(significant=False, default='prod')
    hours = luigi.IntParameter(significant=False, default=8)

    def requires(self):
        for circuit, ncount, ccount, (exec_cores, exec_memory) in \
                itertools.product(self.circuits, self.nodes, self.cores, self.executors):
            yield SpykfuncGanglia(basedir=self.basedir,
                                  cores=ncount * ccount,
                                  circuit=circuit,
                                  version=self.version,
                                  node_cores=ccount,
                                  exec_cores=exec_cores,
                                  exec_memory=exec_memory,
                                  cleanup=self.cleanup,
                                  queue=self.queue,
                                  hours=self.hours)
