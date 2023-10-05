from typing import List, Tuple, Union, Dict
import jobs
from subprocess import run as __run_process

__SQUEUE_PATH = 'squeue'
__SRUN_PATH = 'srun'


def __execute_on_shell(cmd, args):
    process_status = __run_process([cmd] + args, capture_output=True)
    if process_status.returncode > 0:
        print(f'Status {process_status}')
        raise SlurmCallError()
    output = process_status.stdout.decode()
    return output


class EmptyListException(Exception):
    pass


class SlurmCallError(Exception):
    pass


def __list_contains_valid_ids(ids_list):
    if ids_list:
        for item in ids_list:
            if not isinstance(item, int) and not item.is_digit():
                return False
        return True
    else:
        raise EmptyListException()


def __compose_get_processes_status_cmd(job_ids: Union[List, Tuple] = (), job_name: Union[List, Tuple] = ()):
    cmd = ['--states=all', '-h']
    fmt = '%i;%j;%t;%T;%r'
    cmd += ['--format=%s' % fmt]

    if job_ids:
        cmd += [','.join(job_ids)]
    else:
        cmd += ['-a']

    if job_name:
        cmd += ['-n', ','.join(job_name)]

    return cmd


def __execute_squeue(args):
    return __execute_on_shell(__SQUEUE_PATH, args)


def __parse_squeue_output(squeue_output) -> List[jobs.SlurmJobStatus]:
    """
    Parses the output of squeue
    e.g.
    123;test_job;CD;COMPLETED;None
    :param squeue_output:
    :return:
    """
    jobs_found = []
    if squeue_output:
        for line in squeue_output.split('\n'):
            if not line:
                continue
            job_id, job_name, status_code, status, reason = line.split(';')
            jobs_found.append(jobs.SlurmJobStatus(job_id=job_id,
                                                  job_name=job_name,
                                                  status_code=status_code,
                                                  status=status,
                                                  reason=reason))

    return jobs_found


def __map_job_status_per_jobid(job_status_list: List[jobs.SlurmJobStatus]) -> Dict[str, jobs.SlurmJobStatus]:
    return {job_status.job_id: job_status for job_status in job_status_list}


def get_jobs_status(job_ids: Union[List, Tuple] = (), job_name: Union[List, Tuple] = ()) \
        -> Dict[str, jobs.SlurmJobStatus]:
    args = __compose_get_processes_status_cmd(job_ids, job_name)
    output = __execute_squeue(args)
    parsed_output = __parse_squeue_output(output)
    return __map_job_status_per_jobid(parsed_output)


def __execute_srun(args):
    return __execute_on_shell(__SRUN_PATH, args)


def __compose_run_job_arguments(cmd, queue=None, executor_config=None, job_name=None):
    if executor_config:
        raise NotImplementedError()

    args = []
    if queue:
        args += ['-p', queue]
        if 'gpu' in queue.lower():
            args += ['--gres', 'gpu:P100:2']
    if job_name:
        args += ['-J', job_name]
    else:
        args += ['-J', cmd]
    args += cmd.split(' ') if isinstance(cmd, str) else cmd
    return args


# TODO collect output
def run_job(cmd, queue=None, executor_config=None, task_name=None):
    args = __compose_run_job_arguments(cmd, queue, executor_config, task_name)

    _ = __execute_srun(args)
