#!/usr/bin/env python3

import sys, os, asyncio, traceback, time, json
from asyncio.subprocess import create_subprocess_exec, PIPE

from aio_pika import connect, Message, ExchangeType
import pika.exceptions


from clustering import Clustering


class RejectError(Exception): pass
class RejectRequeueError(Exception): pass
class ErrorMessage(Exception): pass
class NoReply(Exception): pass


name = 'STORYLINE-CLUSTERING'

collect_test_data = False
test_data_dir = 'test'
clustering = None


def init(args=None):
    global collect_test_data, test_data_dir
    collect_test_data = args.collect_test_data
    test_data_dir = args.test_data_dir
    if collect_test_data:
        print('Only test data collecting')
        return

    global clustering
    clustering = Clustering('state/state.pickle')


def setup_argparser(parser):
    env = os.environ
    parser.add_argument('--test-data-dir', type=str, default=test_data_dir, help='test data directory')
    parser.add_argument('--collect-test-data', action='store_true', help='collect test data only and write to test data directory')


def shutdown():
    pass


def reset():
    pass


async def process_message(task_data, loop=None, send_reply=None, metadata=None, reject=None, reply=None, routing_key=None,producer_name=None,message=None, **kwargs):
    global collect_test_data, test_data_dir, started, message_registry

    if collect_test_data:
        if not os.path.isdir(test_data_dir):
            os.makedirs(test_data_dir)
        item = metadata.get('itemId', 'unknown')
        filename = os.path.join(test_data_dir, '%s.json' % item)
        print('Writing to %s' % filename)
        with open(filename, 'w') as f:
            json.dump(task_data, f, indent=2)
        # print('Requeueing item.')
        # reject(requeue=True)
        print('Waiting 5 seconds')
        await asyncio.sleep(5)
        # try:
        #     time.sleep(5)   # wait 5 seconds blocking main thread intentionally so no new messages are received
        # except KeyboardInterrupt:
        #     loop.stop()
        # raise NoReply('just took a look, requeued')
        raise RejectError('data collected, rejecting')

    text = task_data.get('text')
    if not text:
        raise ErrorMessage('empty text field')
    if type(text) is not str and not text.get('title') and not text.get('body'):
        raise ErrorMessage('text field missing properties title and body')

    document = {
        'id': task_data.get('id'),
        'text': text if type(text) is str else text.get('body', text.get('title')) # '%s %s' % (text.get('title', ''), text.get('body' '')),
    }

    try:
        response = clustering.add(document)
    except:
        traceback.print_exc()
        raise

    response = dict(cluster_id=response.cluster, document_id=task_data.get('id'), merged_cluster_ids=response.merged)
    await send_reply(response, 'finalResult')
	


def log(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)



if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description='Priberam Clustering Task', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--debug', action='store_true', help='enable debug mode') 
    parser.add_argument('filename', type=str, default='test.json', nargs='?', help='JSON file with task data')

    setup_argparser(parser)

    args = parser.parse_args()

    init(args)

    print('Reading', args.filename)
    with open(args.filename, 'r') as f:
        task_data = json.load(f)
    metadata = {}

    async def print_partial(partial_result):
        print('Partial result:')
        print(partial_result)

    try:
        loop = asyncio.get_event_loop()
        # loop.set_debug(True)
        result = loop.run_until_complete(process_message(task_data, loop=loop, send_reply=print_partial, metadata=metadata))
        print('Result:')
        print(result)
    except KeyboardInterrupt:
        print('INTERRUPTED')
    except:
        print('EXCEPTION')
        traceback.print_exc()
        # raise
