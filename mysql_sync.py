# from binlog_consumer import MaxwellSync
from pydbsync import MaxwellSync
from config import rabbitmq_queue_bind
# import threading
import multiprocessing

def worker(exchange_bind, queue_route):
    sync = MaxwellSync(exchange_bind, queue_route)
    sync.binlog_sync()

def start():
    thread_list = []
    for exchange in rabbitmq_queue_bind:
        ex = exchange.get('exchange')
        queues = exchange.get('queues')
        for q in queues.items():  # q is (queue_name, routing_keys)
            # t = threading.Thread(target=worker, name=q[0], args=(ex, q))
            t = multiprocessing.Process(target=worker, name=q[0], args=(ex, q))
            thread_list.append(t)

    for t in thread_list:
        t.start()

    for t in thread_list:
        t.join()

if __name__ == '__main__':
    start()
