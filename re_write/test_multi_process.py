import multiprocessing
import os
import time

def run_task(name):
    print('Task {0} pid {1} is running, parent id is {2}'.format(name, os.getpid(), os.getppid()))
    time.sleep(1)
    print('Task {0} end.'.format(name))
    return str(name),name * name

if __name__ == '__main__':
    print('current process {0}'.format(os.getpid()))
    p = multiprocessing.Pool(processes=4)
    results = []
    for i in range(6):
        result = p.apply_async(run_task, args=(i,))
        results.append(result)
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    for result in results:
        print(result.get())
    print('All processes done!')


