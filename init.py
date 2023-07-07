from queue import Queue


def worker_queue(number_of_processes) -> Queue:
    """
    Initialization of the worker queue.
    Function returns a queue with all workers that will be available
    """
    queue = Queue(maxsize=number_of_processes-1)
    for i in range(1, number_of_processes):
        queue.put(i)
    return queue


def partition_set(number_of_processes, comm) -> {}:
    """
    Initialization of the set with partitions to work on.
    Initialize with all partitions containing source cells received through the comm
    """
    partitions = set()
    for i in range(number_of_processes - 1):
        message: [tuple] = comm.recv()
        for _partition in message:
            partitions.add(_partition)
    return partitions
