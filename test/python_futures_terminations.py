from concurrent.futures import ThreadPoolExecutor
import time
import sys


def some_worker(message):
    print("{} : Started".format(message))
    try:
        time.sleep(10)
    except KeyboardInterrupt as e:
        print("Something unforseen")
    print("            {} : Stopped".format(message))


def main():
    executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="a-thread")

    try:

        all_futures = []

        for each in range(100):
            # time.sleep(1)
            a_future = executor.submit(some_worker, each)
            all_futures.append(a_future)

        print("Submitted all {} futures".format(len(all_futures)))

    except (KeyboardInterrupt, sys.exit):

        print("Caught while submits")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Cancelled by User".upper())
