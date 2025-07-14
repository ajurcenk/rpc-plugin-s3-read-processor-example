import asyncio
import sys

from redpanda_connect import Message, processor, processor_main, MessageBatch
from redpanda_connect.core import Processor, ProcessorConstructor, Value
from typing import Callable, override, cast
import logging
from typing import Optional, Dict, Any, AsyncGenerator
from aiobotocore.session import get_session
from botocore.exceptions import ClientError, NoCredentialsError


class AsyncS3Reader:
    """
    Async S3 object reader that fetches a single object by its path.
    """

    def __init__(
        self,
        region_name: str = 'us-east-1',
        batch_size: int = 1024 * 1024,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None
    ):
        self.region_name = region_name
        self.batch_size = batch_size
        self.session = get_session()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.client = None
        self._credentials = {
            k: v for k, v in {
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key,
                'aws_session_token': aws_session_token
            }.items() if v is not None
        }


    def is_initialized(self)-> bool:
        return self.client is not None


    async def init_client(self):
        if self.client is None:
            self.logger.info("Creating S3 client")
            self.client = await self.session.create_client(
                's3',
                region_name=self.region_name,
                **self._credentials
            ).__aenter__()

    async def close(self):
        if self.client:
            self.logger.info("Closing S3 client")
            await self.client.__aexit__(None, None, None)
            self.client = None

    async def read_object_in_batches(
        self,
        bucket_name: str,
        object_key: str,
        batch_size: Optional[int] = None
    ) -> AsyncGenerator[bytes, None]:

        await self.init_client()

        effective_batch_size = batch_size or self.batch_size

        try:
            self.logger.info(
                f"Reading object in batches: s3://{bucket_name}/{object_key} (batch_size={effective_batch_size})")

            response = await self.client.get_object(Bucket=bucket_name, Key=object_key)
            body = response['Body']

            batch_count = 0
            total_bytes = 0

            async for chunk in body.iter_chunks(chunk_size=effective_batch_size):
                if chunk:
                    batch_count += 1
                    total_bytes += len(chunk)
                    self.logger.debug(f"Yielding batch {batch_count}: {len(chunk)} bytes")
                    yield chunk

            self.logger.info(
                f"Successfully read {total_bytes} bytes in {batch_count} batches from s3://{bucket_name}/{object_key}")

        except (ClientError, NoCredentialsError) as e:
            self.logger.error(f"Error reading object: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise

    async def read_object(self, bucket_name: str, object_key: str) -> Optional[bytes]:
        chunks = []
        async for chunk in self.read_object_in_batches(bucket_name, object_key):
            chunks.append(chunk)
        return b''.join(chunks)

    async def read_object_as_text(
        self,
        bucket_name: str,
        object_key: str,
        encoding: str = 'utf-8'
    ) -> Optional[str]:
        content = await self.read_object(bucket_name, object_key)
        return content.decode(encoding) if content else None

    async def get_object_metadata(
        self,
        bucket_name: str,
        object_key: str
    ) -> Optional[Dict[str, Any]]:
        await self.init_client()
        try:
            response = await self.client.head_object(Bucket=bucket_name, Key=object_key)
            return {k: v for k, v in response.items() if k != 'ResponseMetadata'}
        except ClientError as e:
            self.logger.error(f"Error getting metadata: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise


class S3ObjectRead(Processor):

    def __init__(self, aws_id, aws_key, aws_region_name, aws_bucket, batch_size, s3_key):
        super().__init__()

        self.logger = logging.getLogger(self.__class__.__name__)
        self.s3_bucket = aws_bucket
        self.s3_key = s3_key

        # Create s3 async reade
        self.reader = AsyncS3Reader(region_name=aws_region_name,
                               aws_access_key_id=aws_id,
                               aws_secret_access_key=aws_key,
                               batch_size=batch_size
                             )



    async def process(self, batch: MessageBatch) -> list[MessageBatch]:

        self.logger.info(f"Inside S3ObjectRead.process()")

        msg_in = batch[0]

        if not self.reader.is_initialized():
            await self.reader.init_client()

        batch_count = 0
        total_size = 0

        self.logger.info(f"Reading object in batches... Input message: {msg_in}")

        s3_object_key = self.s3_key if msg_in.metadata.get('s3_key',None) is None else msg_in.metadata["s3_key"]
        s3_bucket = self.s3_bucket if msg_in.metadata.get('s3_bucket',None) is None else msg_in.metadata["s3_bucket"]
        # TODO Check the input metadata

        out_msg_batch = MessageBatch()
        async for s3_obj_batch in self.reader.read_object_in_batches(
                bucket_name=s3_bucket,
                object_key= s3_object_key
        ):
            out_msg = Message(payload=s3_obj_batch, metadata=msg_in.metadata)
            out_msg_batch.append(out_msg)

            batch_count += 1
            total_size += len(s3_obj_batch)

            self.logger.info(f"Batch {batch_count}: {len(s3_obj_batch)} bytes")

        msg_batches: list[MessageBatch] = list[MessageBatch]()
        msg_batches.append(out_msg_batch)

        return msg_batches

    async def close(self) -> None:
        if self.reader.is_initialized():
            await self.reader.close()


def make_batch_processor_from_class() -> ProcessorConstructor:

    def ctor(config: Value) -> Processor:

        config_map = cast(dict[str, Value], config)

        aws_access_key_id = config_map.get('credentials').get('id')
        aws_secret_access_key = config_map.get('credentials').get('secret')
        region_name = config_map.get('region')


        batch_size = config_map.get('batching').get('batch_size_bytes')
        s3_key = None if config_map.get('object_to_read', None) is None else config_map.get('object_to_read').get('s3_key')
        bucket_name = None if config_map.get('object_to_read', None) is None else config_map.get('object_to_read').get('s3_bucket')

        return S3ObjectRead(aws_access_key_id, aws_secret_access_key, region_name, bucket_name, batch_size, s3_key)


    return ctor

logging.disable(logging.DEBUG)

asyncio.run(processor_main(make_batch_processor_from_class()))