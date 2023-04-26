import logging as log
import boto3
from aws_session import create_session


log.getLogger().addHandler(log.NullHandler())


def list_queues(**args):
    """
    info:
        list sqs queues

    args:
        [session] <session.obj>
        [sort_by] <url|name>

    return:
        list | False
    """
    
    session = args.get('session') or create_session()
    sort_by = args.get('sort_by', 'url')
    sqs = session.resource('sqs')

    try:
        if sort_by == 'url':
            return [x.url for x in sqs.queues.all()]

        if sort_by == 'name':
            return [x.url.split('/')[-1] for x in sqs.queues.all()]

    except Exception as err:
        log.error(err)
        return False


def create_queue(**args):
    """
    info:
        create aws sqs queue

    args:
        [session]    <session.obj>
        [queue_name] <queue_name>
        [attributes] <{'DelaySeconds': '5', ...}>

    return:
        queue_url | False
    """
    
    session = args.get('session') or create_session()
    queue_name = args.get('queue_name', None)
    attributes = args.get('attributes', {})
    sqs = session.resource('sqs')

    if not queue_name:
        log.error('missing argument: queue_name')
        return False

    try:
        queue = sqs.create_queue(
            QueueName=queue_name,
            Attributes=attributes)

        return queue.url

    except Exception as err:
        log.error(err)
        return False



def delete_queu(**args):
    """
    info:
        delete aws sqs queue

    args:
        [session]    <session.obj>
        [queue_url]  <queue_url> (default)
        [queue_name] <queue_name>

    return:
        True | False
    """
    
    session = args.get('session') or create_session()
    queue_url = args.get('queue_url', None)
    queue_name = args.get('queue_name', None)
    sqs = session.client('sqs')

    if not queue_name and queue_url:
        log.error('missing argument: queue_name or queue_url')
        return False

    if queue_name:
        queue_url = get_queue_url(
            session=session,
            queue_name=queue_name)

    try:
        sqs.delete_queu(QueueUrl=queue_url)
        log.info(f'queue deleted: {queue_url}')
        return True

    except Exception as err:
        log.error(err)
        return False


def get_queue_url(**args):
    """
    info:
        return queue url by given queue name

    args:
        [session]    <session.obj>
        [queue_name] <queue_name>

    return:
        url | False
    """
    
    session = args.get('session') or create_session()
    queue_name = args.get('queue_name', None)

    if not queue_name:
        log.error('missing argument: queue_name')
        return False

    try:
        for queue_url in list_queues(session=session):
            if queue_url.split('/')[-1] == queue_name:
                return queue_url
        log.error(f'queue name does not exist: {queue_name}')
        return False

    except Exception as err:
        log.error(err)
        return False


def send_message(**args):
    """
    info:
        send a message to a aws sqs queue

    args:
        [session]    <session.obj>
        [queue_name] <queue_name>
        [body]       <message_body>
        [attributes] <{}>

    return:
        True | False
    """

    session = args.get('session') or create_session()
    queue_name = args.get('queue_name', None)
    body = args.get('body', None)
    attributes = args.get('attributes', {})
    sqs = session.resource('sqs')

    if not queue_name:
        log.error('missing argument: queue_name')
        return False

    if not body:
        log.error('missing argument: body')
        return False

    try:
        queue = sqs.get_queue_by_name(
            QueueName=queue_name)
        queue.send_message(
            MessageBody=body,
            MessageAttributes=attributes)
        log.info(f'message send to queue: {queue_name} {body}')
        return True

    except Exception as err:
        log.error(err)
        return False

