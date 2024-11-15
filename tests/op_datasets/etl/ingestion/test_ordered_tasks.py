from dataclasses import dataclass

from op_analytics.datapipeline.etl.ingestion.task import ordered_task_list


@dataclass
class DummyTask:
    chain: str
    idx: int


def test_ordering():
    op_tasks = [DummyTask("op", i) for i in range(20)]
    fraxtal_tasks = [DummyTask("fraxtal", i) for i in range(3)]
    mode_tasks = [DummyTask("mode", i) for i in range(7)]

    ordered = list(ordered_task_list(op_tasks + fraxtal_tasks + mode_tasks))

    assert ordered == [
        DummyTask(chain="op", idx=0),
        DummyTask(chain="op", idx=1),
        DummyTask(chain="op", idx=2),
        DummyTask(chain="op", idx=3),
        DummyTask(chain="op", idx=4),
        DummyTask(chain="op", idx=5),
        DummyTask(chain="fraxtal", idx=0),
        DummyTask(chain="mode", idx=0),
        DummyTask(chain="mode", idx=1),
        DummyTask(chain="op", idx=6),
        DummyTask(chain="op", idx=7),
        DummyTask(chain="op", idx=8),
        DummyTask(chain="op", idx=9),
        DummyTask(chain="op", idx=10),
        DummyTask(chain="op", idx=11),
        DummyTask(chain="fraxtal", idx=1),
        DummyTask(chain="mode", idx=2),
        DummyTask(chain="mode", idx=3),
        DummyTask(chain="op", idx=12),
        DummyTask(chain="op", idx=13),
        DummyTask(chain="op", idx=14),
        DummyTask(chain="op", idx=15),
        DummyTask(chain="op", idx=16),
        DummyTask(chain="op", idx=17),
        DummyTask(chain="fraxtal", idx=2),
        DummyTask(chain="mode", idx=4),
        DummyTask(chain="mode", idx=5),
        DummyTask(chain="op", idx=18),
        DummyTask(chain="op", idx=19),
        DummyTask(chain="mode", idx=6),
    ]
