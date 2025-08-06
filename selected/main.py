import os
import asyncio
import logging

from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("s3_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("S3")

load_dotenv()

CONFIG = {
    "key_id": os.getenv("KEY_ID"),
    "secret": os.getenv("SECRET"),
    "endpoint": os.getenv("ENDPOINT"),
    "container": os.getenv("CONTAINER"),
}

class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        if not all([key_id, secret, endpoint, container]):
            raise ValueError("Missing required configuration parameters")

        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
            "verify": False,
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection
    
    async def send_file(self, local_source: str):
        file_ref = Path(local_source)
        if not file_ref.exists():
            logger.error(f"File not found locally: {local_source}")
            return
        target_name = file_ref.name
        logger.info(f"Uploading file: {file_ref.name}")
        async with self._connect() as remote:
            with file_ref.open("rb") as binary_data:
                await remote.put_object(
                    Bucket=self._bucket,
                    Key=target_name,
                    Body=binary_data
                )
        logger.info(f"File upload: {file_ref.name}")

    async def fetch_file(self, remote_name: str, local_target: str):
        async with self._connect() as remote:
            response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
            body = await response["Body"].read()
            with open(local_target, "wb") as out:
                out.write(body)
        logger.info(f"Downloaded: {remote_name}")

    async def remove_file(self, remote_name: str):
        async with self._connect() as remote:
            await remote.delete_object(Bucket=self._bucket, Key=remote_name)
        logger.info(f"Removed: {remote_name}")

    async def list_files(self):
        async with self._connect() as s3:
            response = await s3.list_objects_v2(Bucket=self._bucket)
            return [obj['Key'] for obj in response.get('Contents', [])]
    
    async def file_exists(self, remote_name: str):
        async with self._connect() as remote:
            try:
                await remote.head_object(Bucket=self._bucket, Key=remote_name)
                logger.info(f"File exists: {remote_name}") 
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.info(f"File not found: {remote_name}") 
                    return False
                logger.error(f"Error checking file: {str(e)}") 
                raise
    
async def run_demo():
    storage = AsyncObjectStorage(
        key_id= os.getenv("KEY_ID"),
        secret= os.getenv("SECRET"),
        endpoint=os.getenv("ENDPOINT"),
        container= os.getenv("CONTAINER")
    )
    
    await storage.send_file("data.txt")
    await storage.fetch_file("data.txt", "get_data.txt")
    await storage.remove_file("data.txt")

    print(await storage.list_files())
    print(await storage.file_exists("data.txt"))



if __name__ == "__main__":
    asyncio.run(run_demo())



