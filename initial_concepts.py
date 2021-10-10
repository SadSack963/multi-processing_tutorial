# Python Multiprocessing Tutorial: Run Code in Parallel Using the Multiprocessing Module
# Corey Schafer
# https://www.youtube.com/watch?v=fKl2JW_qrso

import time
import multiprocessing
import concurrent.futures


# IO Bound tasks benefit from multi-threading
# CPU Bound tasks benefit from multi-processing

def do_something(seconds=1):
    print(f'Sleeping {seconds} seconds...')
    time.sleep(seconds)
    return f'Done sleeping {seconds} seconds.'


def standard_calls():
    do_something()
    do_something()


def multi():
    # Create two processes
    process_1 = multiprocessing.Process(target=do_something)
    process_2 = multiprocessing.Process(target=do_something)
    # Start the porcesses
    process_1.start()
    process_2.start()
    # Wait for processes to terminate
    process_1.join()
    process_2.join()


def multi_loop():
    processes = []
    seconds = (10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
    for _ in range(10):
        # Unlike with threads, the arguments must be serializable with pickle.
        #   See https://docs.python.org/3/library/pickle.html
        # args=(1.5) -> Expected type 'tuple[Any, ...]', got 'float' instead
        # args=[1.5] -> Expected type 'tuple[Any, ...]', got 'list[float]' instead
        #   args=[1.5] will work, but does generates the warning in PyCharm
        p = multiprocessing.Process(target=do_something, args=(1.5,))  # Must include the comma in the tuple
        p.start()
        processes.append(p)
    for process in processes:
        process.join()


def multi_pool():
    # Best used with a context manager (with)
    with concurrent.futures.ProcessPoolExecutor() as ppe:
        # submit() returns a future object
        process_1 = ppe.submit(do_something, 1.5)
        process_2 = ppe.submit(do_something, 1.5)
        print(process_1.result())
        print(process_2.result())


def multi_pool_loop():
    second_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    with concurrent.futures.ProcessPoolExecutor() as ppe:
        # submit() returns a future object
        processes = [ppe.submit(do_something, second) for second in second_list]
        # Inside the with block, this prints each result as it completes
        for process in concurrent.futures.as_completed(processes):
            # NOTE that exceptions are raised when the result is returned!
            # So exceptions need to be handled here
            print(process.result())
    # # This prints results in a random order
    # for process in concurrent.futures.as_completed(processes):
    #     print(process.result())


def multi_pool_map():
    second_list = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    with concurrent.futures.ProcessPoolExecutor() as ppe:
        futures = ppe.map(do_something, second_list)
        # map() returns the results in the order that they were started
        for future in futures:
            # NOTE that exceptions are raised when the result is returned!
            # So exceptions need to be handled here
            print(future)


if __name__ == '__main__':
    start = time.perf_counter()

    # Synchronous task
    # ================
    # standard_calls()

    # Multi-processing
    # ================
    # multi()
    # multi_loop()
    # multi_pool()
    # multi_pool_loop()
    multi_pool_map()

    finish = time.perf_counter()
    print(f'Finished in {round(finish - start, 2)} second.')
