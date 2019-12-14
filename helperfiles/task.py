import os
from luigi.local_target import LocalTarget


class Requirement:
    def __init__(self, task_class, **params):
        self.task_class = task_class
        self.params = params

    def __get__(self, task, cls):
        if task is None:
            pass
        else:
            return task.clone(self.task_class, **self.params)


class Requires:
    """Composition to replace :meth:`luigi.task.Task.requires"""

    def __get__(self, task, cls):
        if task is None:
            return self

        # Bind self/task in a closure
        return lambda: self(task)

    def __call__(self, task):
        """Returns the requirements of a task

        Assumes the task class has :class:`.Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.

        :returns: requirements compatible with `task.requires()`
        :rtype: dict
        """
        return {
            k: getattr(task, k)
            for k, v in task.__class__.__dict__.items()
            if isinstance(v, Requirement)
        }


class TargetOutput:
    def __init__(
        self,
        file_pattern="{task.__class__.__name__}",
        target_class=LocalTarget,
        **target_kwargs
    ):
        self.file_pattern = file_pattern
        self.target_class = target_class
        self.params = target_kwargs

    def __get__(self, task, cls):
        if task is None:
            return self
        return lambda: self(task)

    def __call__(self, task=LocalTarget):
        # Determine the path etc here
        task = task
        file_pattern = self.file_pattern
        path = os.path.join(file_pattern)
        return self.target_class(path)
