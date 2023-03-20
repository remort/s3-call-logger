import argparse
import logging
from contextlib import contextmanager
from typing import Iterator

import boto3
import docker
import watchtower
from botocore.exceptions import BotoCoreError
from docker.client import DockerClient
from docker.errors import APIError, ImageNotFound
from docker.types.daemon import CancellableStream

log = logging.getLogger('docker_2_aws')
logging.basicConfig(level='DEBUG')
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('docker').setLevel(logging.INFO)

parser = argparse.ArgumentParser(description='Run a command inside a container and log it to AWS cloud.')
parser.add_argument('--docker-image', help='Docker image name', required=True)
parser.add_argument('--bash-command', help='Command to run inside container', required=True)
parser.add_argument('--aws-cloudwatch-group', help='aws cloudwatch group', required=True)
parser.add_argument('--aws-cloudwatch-stream', help='aws cloudwatch stream', required=True)
parser.add_argument('--aws-access-key-id', help='aws access key id', required=True)
parser.add_argument('--aws-secret-access-key', help='aws secret access key', required=True)
parser.add_argument('--aws-region', help='aws region', required=True)
parser.parse_args()


@contextmanager
def get_aws_logger(
    aws_cloudwatch_group: str,
    aws_cloudwatch_stream: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str,
) -> logging.Logger | None:
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region,
    )

    logger = logging.getLogger('aws_logger')
    try:
        handler = watchtower.CloudWatchLogHandler(
            boto3_client=session.client('logs'),
            log_group_name=aws_cloudwatch_group,
            log_stream_name=aws_cloudwatch_stream,
            create_log_group=True,
            create_log_stream=True,
            use_queues=True,
            send_interval=5,

        )
    except watchtower.WatchtowerError as exc:
        log.error(f'Unable to obtain AWS logger with given credentials. Error: {exc}')
        return
    logger.addHandler(handler)

    try:
        yield logger
    finally:
        log.info('Flushing and closing AWS logger.')
        handler.flush()
        handler.close()


def find_or_pull_image(client: DockerClient, image_name: str) -> bool:
    log.info(f'Find or pull image {image_name}')

    try:
        client.images.get(image_name)
        return True
    except ImageNotFound:
        log.debug(f'Image {image_name} is not found locally')
        return False
    except APIError:
        log.error(f'Call to get image locally failed')

    log.debug(f'Image {image_name} not found. Pulling it...')
    try:
        client.images.pull(image_name)
        log.debug(f'Image {image_name} is pulled')
        return True
    except APIError as exc:
        log.warning(f'Unable to pull image {image_name}, error is: {exc}')
        return False


@contextmanager
def run_command(client: DockerClient, image_name: str, command: str) -> CancellableStream:
    log.info(f'Run command in container {image_name}')
    command = f"sh -c '{command}'"
    container = client.containers.run(image=image_name, command=command, remove=True, detach=True, tty=True)
    log_stream = container.logs(stream=True)

    try:
        yield log_stream
    finally:
        log.info('Closing and stopping container.')
        log_stream.close()
        container.stop()


def readline(stream: CancellableStream) -> Iterator[str]:
    """
    If we use tty=True running command in docker container,
      we receive a byte chars stream witn lines divided by \r and \n bytes.

    If command launches a subshell in the container,
      we get buffered logging and need to wait for the buffer chunk being filled to get log records.

    In order to achieve unbuffered logging with subshells we must add tty=True.
    However, in this case we have to deal with a stream of byte chars, where lines divided by b'\r' and b'\n' chars.

    This function continuously reads this stream of bytes yielding lines of text upon full line arrival.
    """
    line = []
    prev_char = None
    for char in stream:
        if char == b'\n':
            if prev_char == b'\r':
                yield b''.join(line).decode('utf-8')
                line = []
                prev_char = None
        else:
            line.append(char)
            prev_char = char


def log_command_output_to_aws(aws_logger: logging.Logger, log_stream: CancellableStream) -> None:
    log.info(f'Log command output to AWS CloudWatch')
    for line in readline(log_stream):
        try:
            aws_logger.info(line)
        except BotoCoreError as exc:
            log.error(f'AWS API error in logging line {line} to AWS CloudWatch: {exc}')
        except Exception as exc:
            log.error(f'General error in logging line {line} to AWS CloudWatch: {exc}')


def run() -> None:
    params = parser.parse_args()
    client = docker.from_env()

    with get_aws_logger(
        params.aws_cloudwatch_group,
        params.aws_cloudwatch_stream,
        params.aws_access_key_id,
        params.aws_secret_access_key,
        params.aws_region,
    ) as aws_logger:
        if not aws_logger:
            return

        if find_or_pull_image(client, params.docker_image) is False:
            return

        with run_command(client, params.docker_image, params.bash_command) as log_stream:
            log_command_output_to_aws(aws_logger, log_stream)


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        log.info('Interrupted by user.')
    except Exception as err:
        log.error(f'Unexpected critical error: {err}')
