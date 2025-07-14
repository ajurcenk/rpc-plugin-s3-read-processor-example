
import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import cast
import boto3
import time
import pytz
import signal
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import logging
from redpanda_connect import Message, Value, input, input_main
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any


AWS_CONFIG = {
    'aws_access_key_id': '',
    'aws_secret_access_key': '',
    'region_name': 'us-east-2'
}

TIME_THRESHOLD_HOURS = 10  # Look back 1 hour for changes
MAX_OBJECTS_TO_DISPLAY = 50  # Limit display to avoid overwhelming output

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('s3_monitor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TimestampUtils:
    """Utility class for timestamp conversions in bookmarks."""

    @staticmethod
    def to_utc(dt: datetime, source_timezone: Optional[str] = None) -> datetime:
        """
        Convert datetime to UTC timezone.

        Args:
            dt: Datetime object to convert
            source_timezone: Source timezone (e.g., 'US/Eastern', 'Europe/London').
                           If None, assumes local system timezone for naive datetime,
                           or uses existing timezone info for aware datetime.

        Returns:
            Datetime object in UTC timezone
        """
        if dt.tzinfo is None:
            # Naive datetime - assume it's in the specified timezone or local timezone
            if source_timezone:
                source_tz = pytz.timezone(source_timezone)
                dt = source_tz.localize(dt)
            else:
                # Assume local timezone
                dt = dt.replace(tzinfo=timezone.utc)

        # Convert to UTC
        return dt.astimezone(timezone.utc)

    @staticmethod
    def from_utc(dt: datetime, target_timezone: str) -> datetime:
        """
        Convert UTC datetime to specified timezone.

        Args:
            dt: UTC datetime object
            target_timezone: Target timezone (e.g., 'US/Eastern', 'Asia/Tokyo')

        Returns:
            Datetime object in target timezone
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        target_tz = pytz.timezone(target_timezone)
        return dt.astimezone(target_tz)

    @staticmethod
    def now_utc() -> datetime:
        """Get current datetime in UTC."""
        return datetime.now(timezone.utc)

    @staticmethod
    def format_utc_iso(dt: datetime) -> str:
        """Format datetime as ISO string with UTC timezone."""
        utc_dt = TimestampUtils.to_utc(dt)
        return utc_dt.isoformat()

@dataclass
class Bookmark:
    """
    Represents a single bookmark entry for a topic-partition combination.
    """
    topic: str
    partition: str
    offset: int
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def timestamp_utc(self) -> datetime:
        """Get timestamp in UTC timezone."""
        # TODO Store the datetime in the UTC time and remove UTC conversion logic
        return TimestampUtils.to_utc(self.timestamp, 'US/Eastern')

    def timestamp_utc_iso(self) -> str:
        """Get timestamp as ISO string in UTC."""
        return TimestampUtils.format_utc_iso(self.timestamp)

    def __post_init__(self):
        """Validate bookmark data after initialization."""
        if not isinstance(self.topic, str) or not self.topic.strip():
            raise ValueError("Topic must be a non-empty string")
        if not isinstance(self.partition, str) or not self.partition.strip():
            raise ValueError("Partition must be a non-empty string")
        if not isinstance(self.offset, int) or self.offset < 0:
            raise ValueError("Offset must be a non-negative integer")

    def to_dict(self) -> Dict[str, Any]:
        """Convert bookmark to dictionary format."""
        return {
            'topic': self.topic,
            'partition': self.partition,
            'offset': self.offset,
            'timestamp': self.timestamp.isoformat(),

            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Bookmark':
        """Create bookmark from dictionary."""
        return cls(
            topic=data['topic'],
            partition=data['partition'],
            offset=data['offset'],
            timestamp=datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat())),
            metadata=data.get('metadata', {})
        )


class BookmarkManager:
    """
    Manages bookmarks for data consumers, tracking current position
    across multiple topics and partitions.
    """

    def __init__(self, consumer_id: str, storage_path: Optional[str] = None):
        """
        Initialize bookmark manager.

        Args:
            consumer_id: Unique identifier for the consumer
            storage_path: Optional path for persistent storage
        """
        self.consumer_id = consumer_id
        self.storage_path = Path(storage_path) if storage_path else None
        self._bookmarks: Dict[str, Bookmark] = {}
        self._load_bookmarks()

    def _get_bookmark_key(self, topic: str, partition: str) -> str:
        """Generate unique key for topic-partition combination."""
        return f"{topic}:{partition}"

    def set_bookmark(self, topic: str, partition: str, offset: int,
                     metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Set or update bookmark for a topic-partition.

        Args:
            topic: Topic name
            partition: Partition identifier
            offset: Current offset position
            metadata: Optional metadata to store with bookmark
        """
        key = self._get_bookmark_key(topic, partition)
        bookmark = Bookmark(
            topic=topic,
            partition=partition,
            offset=offset,
            metadata=metadata or {}
        )
        self._bookmarks[key] = bookmark
        self._save_bookmarks()

    def get_bookmark(self, topic: str, partition: str) -> Optional[Bookmark]:
        """
        Get bookmark for a topic-partition.

        Args:
            topic: Topic name
            partition: Partition identifier

        Returns:
            Bookmark if exists, None otherwise
        """
        key = self._get_bookmark_key(topic, partition)
        return self._bookmarks.get(key)

    def get_offset(self, topic: str, partition: str) -> Optional[int]:
        """
        Get current offset for a topic-partition.

        Args:
            topic: Topic name
            partition: Partition identifier

        Returns:
            Current offset if bookmark exists, None otherwise
        """
        bookmark = self.get_bookmark(topic, partition)
        return bookmark.offset if bookmark else None

    def get_all_bookmarks(self) -> List[Bookmark]:
        """Get all bookmarks as a list."""
        return list(self._bookmarks.values())

    def get_bookmarks_for_topic(self, topic: str) -> List[Bookmark]:
        """
        Get all bookmarks for a specific topic.

        Args:
            topic: Topic name

        Returns:
            List of bookmarks for the topic
        """
        return [bookmark for bookmark in self._bookmarks.values()
                if bookmark.topic == topic]

    def get_topics(self) -> List[str]:
        """Get list of all topics with bookmarks."""
        return list(set(bookmark.topic for bookmark in self._bookmarks.values()))

    def get_partitions_for_topic(self, topic: str) -> List[str]:
        """
        Get list of partitions for a topic.

        Args:
            topic: Topic name

        Returns:
            List of partition identifiers
        """
        return [bookmark.partition for bookmark in self._bookmarks.values()
                if bookmark.topic == topic]

    def delete_bookmark(self, topic: str, partition: str) -> bool:
        """
        Delete bookmark for a topic-partition.

        Args:
            topic: Topic name
            partition: Partition identifier

        Returns:
            True if bookmark was deleted, False if not found
        """
        key = self._get_bookmark_key(topic, partition)
        if key in self._bookmarks:
            del self._bookmarks[key]
            self._save_bookmarks()
            return True
        return False

    def delete_topic_bookmarks(self, topic: str) -> int:
        """
        Delete all bookmarks for a topic.

        Args:
            topic: Topic name

        Returns:
            Number of bookmarks deleted
        """
        keys_to_delete = [key for key, bookmark in self._bookmarks.items()
                          if bookmark.topic == topic]
        for key in keys_to_delete:
            del self._bookmarks[key]

        if keys_to_delete:
            self._save_bookmarks()

        return len(keys_to_delete)

    def clear_all_bookmarks(self) -> None:
        """Clear all bookmarks."""
        self._bookmarks.clear()
        self._save_bookmarks()

    def update_offset(self, topic: str, partition: str, offset: int) -> None:
        """
        Update only the offset for an existing bookmark.

        Args:
            topic: Topic name
            partition: Partition identifier
            offset: New offset value
        """
        key = self._get_bookmark_key(topic, partition)
        if key in self._bookmarks:
            self._bookmarks[key].offset = offset
            self._bookmarks[key].timestamp = datetime.now()
            self._save_bookmarks()
        else:
            self.set_bookmark(topic, partition, offset)

    def get_latest_bookmark_by_timestamp(self) -> Optional[Bookmark]:
        """
        Get the bookmark with the most recent timestamp across all topics.

        Returns:
            Bookmark with latest timestamp, or None if no bookmarks exist
        """
        if not self._bookmarks:
            return None

        return max(self._bookmarks.values(), key=lambda b: b.timestamp)

    def get_latest_topic_by_timestamp(self) -> Optional[str]:
        """
        Get the topic name that has the most recent bookmark timestamp.

        Returns:
            Topic name with latest bookmark, or None if no bookmarks exist
        """
        latest_bookmark = self.get_latest_bookmark_by_timestamp()
        return latest_bookmark.topic if latest_bookmark else None

    def get_latest_bookmark_for_topic(self, topic: str) -> Optional[Bookmark]:
        """
        Get the bookmark with the most recent timestamp for a specific topic.

        Args:
            topic: Topic name

        Returns:
            Latest bookmark for the topic, or None if topic has no bookmarks
        """
        topic_bookmarks = self.get_bookmarks_for_topic(topic)
        if not topic_bookmarks:
            return None

        return max(topic_bookmarks, key=lambda b: b.timestamp)

    def get_oldest_bookmark_by_timestamp(self) -> Optional[Bookmark]:
        """
        Get the bookmark with the oldest timestamp across all topics.

        Returns:
            Bookmark with oldest timestamp, or None if no bookmarks exist
        """
        if not self._bookmarks:
            return None

        return min(self._bookmarks.values(), key=lambda b: b.timestamp)

    def get_bookmarks_sorted_by_timestamp(self, ascending: bool = True) -> List[Bookmark]:
        """
        Get all bookmarks sorted by timestamp.

        Args:
            ascending: If True, sort oldest to newest. If False, newest to oldest.

        Returns:
            List of bookmarks sorted by timestamp
        """
        return sorted(self._bookmarks.values(),
                      key=lambda b: b.timestamp,
                      reverse=not ascending)

    def get_bookmark_summary(self) -> Dict[str, Any]:
        """
        Get summary of all bookmarks.

        Returns:
            Dictionary with bookmark statistics
        """
        topics = self.get_topics()
        total_bookmarks = len(self._bookmarks)

        topic_stats = {}
        for topic in topics:
            bookmarks = self.get_bookmarks_for_topic(topic)
            topic_stats[topic] = {
                'partition_count': len(bookmarks),
                'total_offset': sum(b.offset for b in bookmarks),
                'latest_timestamp': max(b.timestamp for b in bookmarks) if bookmarks else None
            }

        return {
            'consumer_id': self.consumer_id,
            'total_bookmarks': total_bookmarks,
            'topic_count': len(topics),
            'topics': topic_stats
        }

    def _save_bookmarks(self) -> None:
        """Save bookmarks to persistent storage if configured."""
        if not self.storage_path:
            return

        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)

            # Save as JSON
            data = {
                'consumer_id': self.consumer_id,
                'bookmarks': [bookmark.to_dict() for bookmark in self._bookmarks.values()]
            }

            with open(self.storage_path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save bookmarks: {e}")

    def _load_bookmarks(self) -> None:
        """Load bookmarks from persistent storage if available."""
        if not self.storage_path or not self.storage_path.exists():
            return

        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)

            for bookmark_data in data.get('bookmarks', []):
                bookmark = Bookmark.from_dict(bookmark_data)
                key = self._get_bookmark_key(bookmark.topic, bookmark.partition)
                self._bookmarks[key] = bookmark
        except Exception as e:
            print(f"Warning: Failed to load bookmarks: {e}")

    def __str__(self) -> str:
        """String representation of bookmark manager."""
        return f"BookmarkManager(consumer_id={self.consumer_id}, bookmarks={len(self._bookmarks)})"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return (f"BookmarkManager(consumer_id='{self.consumer_id}', "
                f"bookmarks={len(self._bookmarks)}, "
                f"topics={self.get_topics()})")


@dataclass
class AWSKeyObject:
    """
    Data class representing AWS S3 key object attributes.

    This class encapsulates the common attributes returned when
    working with AWS S3 objects through boto3 or similar libraries.
    """

    # Core object identifiers
    key: str  # Object key (path/filename)
    bucket: str  # S3 bucket name

    # Object metadata
    size: int  # Object size in bytes
    last_modified: datetime  # Last modification timestamp
    etag: str  # Entity tag (MD5 hash)

    # Storage information
    storage_class: str = "STANDARD"  # Storage class (STANDARD, IA, GLACIER, etc.)

    # Optional attributes
    version_id: Optional[str] = None  # Object version ID (if versioning enabled)
    content_type: Optional[str] = None  # MIME type
    content_encoding: Optional[str] = None
    content_disposition: Optional[str] = None
    cache_control: Optional[str] = None
    expires: Optional[datetime] = None

    # Encryption information
    server_side_encryption: Optional[str] = None  # AES256, aws:kms, etc.
    kms_key_id: Optional[str] = None

    # Access control
    owner_id: Optional[str] = None
    owner_display_name: Optional[str] = None

    # Additional metadata
    metadata: Optional[Dict[str, Any]] = None  # User-defined metadata

    def __post_init__(self):
        """Initialize default values and validate data."""
        if self.metadata is None:
            self.metadata = {}

    @property
    def size_mb(self) -> float:
        """Return size in megabytes."""
        return self.size / (1024 * 1024)

    @property
    def size_gb(self) -> float:
        """Return size in gigabytes."""
        return self.size / (1024 * 1024 * 1024)

    @property
    def s3_uri(self) -> str:
        """Return S3 URI format."""
        return f"s3://{self.bucket}/{self.key}"

    @property
    def is_encrypted(self) -> bool:
        """Check if object is encrypted."""
        return self.server_side_encryption is not None

    @classmethod
    def from_boto3_response(cls, response: Dict[str, Any], bucket: str) -> 'AWSKeyObject':
        """
        Create AWSKeyObject from boto3 list_objects_v2 or head_object response.

        Args:
            response: Dict containing object metadata from boto3
            bucket: S3 bucket name

        Returns:
            AWSKeyObject instance
        """
        return cls(
            key=response.get('Key', ''),
            bucket=bucket,
            size=response.get('Size', 0),
            last_modified=response.get('LastModified', datetime.now()),
            etag=response.get('ETag', '').strip('"'),
            storage_class=response.get('StorageClass', 'STANDARD'),
            version_id=response.get('VersionId'),
            content_type=response.get('ContentType'),
            content_encoding=response.get('ContentEncoding'),
            content_disposition=response.get('ContentDisposition'),
            cache_control=response.get('CacheControl'),
            expires=response.get('Expires'),
            server_side_encryption=response.get('ServerSideEncryption'),
            kms_key_id=response.get('SSEKMSKeyId'),
            owner_id=response.get('Owner', {}).get('ID'),
            owner_display_name=response.get('Owner', {}).get('DisplayName'),
            metadata=response.get('Metadata', {})
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            'key': self.key,
            'bucket': self.bucket,
            'size': self.size,
            'size_mb': self.size_mb,
            'size_gb': self.size_gb,
            'last_modified': self.last_modified.isoformat(),
            'etag': self.etag,
            'storage_class': self.storage_class,
            'version_id': self.version_id,
            'content_type': self.content_type,
            'content_encoding': self.content_encoding,
            'content_disposition': self.content_disposition,
            'cache_control': self.cache_control,
            'expires': self.expires.isoformat() if self.expires else None,
            'server_side_encryption': self.server_side_encryption,
            'kms_key_id': self.kms_key_id,
            'owner_id': self.owner_id,
            'owner_display_name': self.owner_display_name,
            'metadata': self.metadata,
            's3_uri': self.s3_uri,
            'is_encrypted': self.is_encrypted
        }


class AsyncS3ChangeMonitor:
    def __init__(self, bucket_name: str, aws_config: dict, queue: asyncio, last_check_time = None, check_interval_min = 1):
        self.bucket_name = bucket_name
        self.aws_config = aws_config
        self.running = True
        self.last_check_time = last_check_time
        self.s3_client = None
        self.queue = queue
        self.check_interval_min = check_interval_min

    def initialize_s3_client(self) -> bool:
        """Initialize S3 client with error handling"""
        try:
            session = boto3.Session(**self.aws_config)
            self.s3_client = session.client('s3')

            # Test the connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            return False

    def get_recent_changes(self, hours_back: float = 1.0) -> List[Dict]:
        """Get objects modified within the specified time threshold or since last check"""
        try:
            # Use last_check_time if it exists, otherwise use hours_back
            if self.last_check_time is not None:
                cutoff_time = self.last_check_time
                logger.info(f"Checking for changes since last check: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            else:
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
                logger.info(
                    f"Initial check - looking back {hours_back} hours since: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

            recent_changes = []
            total_objects = 0

            # Get all objects using pagination
            paginator = self.s3_client.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=self.bucket_name):
                contents = page.get('Contents', [])
                total_objects += len(contents)

                for obj in contents:
                    last_modified = obj['LastModified']
                    if last_modified > cutoff_time:

                        aws_obj = AWSKeyObject.from_boto3_response(obj, self.bucket_name)
                        aws_obj_map = aws_obj.to_dict()

                        recent_changes.append(aws_obj_map)

            # Sort by last modified time (oldest first)
            recent_changes.sort(key=lambda x: x['last_modified'], reverse=False)

            logger.info(f"Scanned {total_objects} objects, found {len(recent_changes)} recent changes")

            return recent_changes

        except Exception as e:
            logger.error(f"Error getting recent changes: {str(e)}")
            return []

    def format_file_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format"""
        if size_bytes == 0:
            return "0 B"

        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        size = float(size_bytes)

        while size >= 1024.0 and i < len(size_names) - 1:
            size /= 1024.0
            i += 1

        return f"{size:.2f} {size_names[i]}"

    def display_changes(self, changes: List[Dict]):
        """Display the changes in a formatted way"""
        if not changes:
            logger.info("No recent changes found")
            return

        logger.info(f"Found {len(changes)} recent changes:")
        print("\n" + "=" * 80)
        print(f"{'FILE NAME':<40} {'MODIFIED':<20} {'SIZE':<10} {'TYPE':<10}")
        print("=" * 80)

        displayed_count = 0
        for change in changes:
            if displayed_count >= MAX_OBJECTS_TO_DISPLAY:
                logger.info(f"... and {len(changes) - displayed_count} more changes (limit reached)")
                break

            modified_time = change['last_modified']
            file_size = self.format_file_size(change['size'])

            # Truncate long file names
            file_name = change['key']
            if len(file_name) > 38:
                file_name = file_name[:35] + "..."

            print(f"{file_name:<40} {modified_time:<20} {file_size:<10} {change['storage_class']:<10}")
            displayed_count += 1

        print("=" * 80 + "\n")

    async def write_log_entry(self, changes: List[Dict]):
        """Write detailed log entry to file (kept for potential future use)"""
        pass

    def check_for_changes(self)-> list[dict] | None:
        """Main function to check for changes"""
        try:
            current_time = datetime.now(timezone.utc)
            logger.info(f"Starting change check at {current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

            # Get recent changes (will use last_check_time if available)
            changes = self.get_recent_changes(TIME_THRESHOLD_HOURS)

            # Display changes
            # self.display_changes(changes)

            # Update last check time AFTER successful check
            self.last_check_time = current_time

            # Log summary
            if changes:
                logger.info(f"Change check completed - {len(changes)} changes found")
            else:
                logger.info("Change check completed - no changes found")
            return changes
        except Exception as e:
            logger.error(f"Error during change check: {str(e)}")
            return None
            # Don't update last_check_time if there was an error

    def signal_handler(self, sig, frame):
        """Handle graceful shutdown"""
        logger.info("Received shutdown signal, stopping monitor...")
        self.running = False

    async def run_monitor(self):
        """Run the periodic async monitor"""
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Initialize S3 client
        loop = asyncio.get_event_loop()
        init_result = await loop.run_in_executor(None, self.initialize_s3_client)
        if not init_result:
            logger.error("Failed to initialize S3 client. Exiting.")
            return

        logger.info(f"Starting Async S3 Change Monitor for bucket: {self.bucket_name}")
        logger.info(f"Time threshold: {TIME_THRESHOLD_HOURS} hours")
        logger.info(f"Check interval: {self.check_interval_min} minutes")
        logger.info("Press Ctrl+C to stop\n")

        # Run initial check
        changes = await loop.run_in_executor(None, self.check_for_changes)
        if changes is not None:
            await self.queue.put(changes)

        # Run periodic checks
        while self.running:
            try:
                # Use asyncio.sleep for non-blocking wait
                await asyncio.sleep(self.check_interval_min * 60)

                if self.running:  # Check if we're still running after sleep
                    changes = await loop.run_in_executor(None, self.check_for_changes)
                    if changes is not None:
                        await self.queue.put(changes)


            except asyncio.CancelledError:
                logger.info("Monitor task cancelled, stopping...")
                break
            except Exception as e:
                logger.error(f"Unexpected error in monitor loop: {str(e)}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying

        logger.info("Monitor stopped")


@input
async def monitor_s3_objects(config: Value) -> AsyncIterator[Message]:

    logger.info(f"Configuration: {config}")

    # TODO Check configuration

    # Override config defaults:
    config_map = cast(dict[str, Value], config)
    AWS_CONFIG['aws_access_key_id'] = config_map.get('credentials').get('id')
    AWS_CONFIG['aws_secret_access_key'] = config_map.get('credentials').get('secret')
    AWS_CONFIG['region_name'] = config_map.get('region')

    bucket_name = config_map.get('bucket')
    bookmarks_file_path = config_map.get('bookmark_manager').get('file_path')
    check_interval_min = config_map.get('check_interval_min', 1)

    # TODO Only for demo, we need to support ack function

    # Restore bookmarks
    manager : BookmarkManager = BookmarkManager("consumer-001", bookmarks_file_path)
    latest_bookmark: Optional[Bookmark] = manager.get_latest_bookmark_for_topic(bucket_name)

    logger.info(f"Latest bookmark: {latest_bookmark} for topic {bucket_name}")

    cut_of_time = datetime.now(timezone.utc) - timedelta(hours=TIME_THRESHOLD_HOURS)
    if latest_bookmark is not None:
        cut_of_time = latest_bookmark.timestamp_utc
        logger.info(f"cut_of_time: {cut_of_time}")
        # datetime.now(timezone.utc)
            # latest_bookmark.timestamp)

    queue = asyncio.Queue()
    monitor = AsyncS3ChangeMonitor(bucket_name, AWS_CONFIG, queue, cut_of_time, check_interval_min)

    # TODO Stop producer task on the input shutdown
    producer_tasks = asyncio.create_task(monitor.run_monitor())

    while True:
        # Wait for s3 changes
        item: list[dict]  = await queue.get()
        if item is None:
            break
        for obj_change_evnt in item:
            yield Message(payload={"object_info": obj_change_evnt})
            # Update bookmark
            manager.set_bookmark(bucket_name, obj_change_evnt["key"], 0)
            #
        queue.task_done()

        # Stop async tasks on the input stop
asyncio.run(input_main(monitor_s3_objects))