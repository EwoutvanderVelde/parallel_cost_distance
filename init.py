from queue import Queue


def worker_queue(number_of_processes) -> Queue:
    queue = Queue(maxsize=number_of_processes-1)
    for i in range(1, number_of_processes):
        queue.put(i)
    return queue


def partition_set(number_of_processes, comm) -> {}:
    partitions = set()
    for i in range(number_of_processes - 1):
        message: [tuple] = comm.recv()
        for _partition in message:
            partitions.add(_partition)
    return partitions
