"""Extrac
"""
import json
import logging


L = logging.getLogger(__name__)


class Stage(object):
    """Dummy object representing a stage
    """
    __slots__ = ["input_rows", "output_rows", "shuffle_read", "shuffle_write"]

    def __init__(self):
        self.output_rows = 0
        self.shuffle_read = 0
        self.shuffle_write = 0

    def __repr__(self):
        return f"<Stage shuffle={self.shuffle_read}, {self.shuffle_write} " \
               f"out={self.output_rows}>"


class EventLog(object):
    """Process an event log from Apache Spark
    """
    def __init__(self, fn):
        self._filename = fn
        self._stages = {}

        self._events_seen = set()
        self._metrics_seen = set()

        self._processors = {
            "SparkListenerStageCompleted": self._process_stage,
            # "SparkListenerTaskEnd": self._process_task
        }
        self._processed = False

    def _load_data(self):
        """Load data from the file if not done already
        """
        if self._processed:
            return
        with open(self._filename) as fd:
            for line in fd:
                data = json.loads(line)
                event = data["Event"]
                if event not in self._events_seen:
                    L.debug("available event: %s", event)
                    self._events_seen.add(event)
                fct = self._processors.get(event, self._process_event)
                fct(data)
        self._processed = True

    def _process_stage(self, data):
        """Extract stage metrics
        """
        id_ = data["Stage Info"]["Stage ID"]
        stage = Stage()
        for metric in data["Stage Info"]["Accumulables"]:
            name = metric["Name"]
            if name not in self._metrics_seen:
                L.debug("available metric: %s", name)
                self._metrics_seen.add(name)
            if name == "internal.metrics.shuffle.write.bytesWritten":
                stage.shuffle_write = metric["Value"]
            elif name == "internal.metrics.shuffle.read.localBytesRead":
                stage.shuffle_read += metric["Value"]
            elif name == "internal.metrics.shuffle.read.remoteBytesRead":
                stage.shuffle_read += metric["Value"]
            elif name == "number of output rows":
                stage.output_rows = int(metric["Value"])
        self._stages[id_] = stage

    def _process_event(self, data):
        """Dummy default method
        """
        pass

    @property
    def shuffle_size(self):
        """:property: the maximum shuffle size seen
        """
        self._load_data()
        return max([0] + [max(s.shuffle_read, s.shuffle_write) for s in self._stages.values()])

    @property
    def max_rows(self):
        """:property: the number of output records of the last stage having measured some
        """
        self._load_data()
        return max([0] + [s.output_rows for s in self._stages.values()])

    @property
    def last_rows(self):
        """:property: the number of output records of the last stage having measured some
        """
        self._load_data()
        for s in sorted(self._stages, reverse=True):
            if self._stages[s].output_rows > 0:
                return self._stages[s].output_rows
        return 0


if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.DEBUG)
    for fn in sys.argv[1:]:
        log = EventLog(fn)
        print(f"Shuffle size:   {log.shuffle_size:20d}")
        print(f"Output rows:    {log.last_rows:20d}")
        print(f"Processed rows: {log.max_rows:20d}")
        for s in log._stages.values():
            print(s)
