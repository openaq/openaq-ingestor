import boto3
import logging
import psycopg2
from .settings import settings
from .lcs import load_measurements_db, load_metadata_db
from .fetch import load_db
from time import time
import json

from datetime import datetime, timezone

s3c = boto3.client("s3")

logger = logging.getLogger('handler')

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


def handler(event, context):
    logger.debug(event)
    records = event.get("Records")
    if records is not None:
        try:
            with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
                with connection.cursor() as cursor:
                    connection.set_session(autocommit=True)
                    for record in records:
                        if record['EventSource'] == 'aws:sns':
                            keys = getKeysFromSnsRecord(record)
                        else:
                            keys = getKeysFromS3Record(record)

                        logger.debug(keys)
                        for obj in keys:
                            bucket = obj['bucket']
                            key = obj['key']
                            lov2 = s3c.list_objects_v2(
                                Bucket=bucket, Prefix=key, MaxKeys=1
                            )

                            try:
                                file_size = lov2["Contents"][0]["Size"]
                                last_modified = lov2["Contents"][0]["LastModified"]
                            except KeyError:
                                logger.error("""
                                could not get info from obj
                                """)
                                file_size = None
                                last_modified = datetime.now().replace(
                                    tzinfo=timezone.utc
                                )

                            cursor.execute(
                                """
                                INSERT INTO fetchlogs (key
                                , file_size
                                , last_modified
                                )
                                VALUES(%s, %s, %s)
                                ON CONFLICT (key) DO UPDATE
                                SET last_modified=EXCLUDED.last_modified,
                                completed_datetime=NULL RETURNING *;
                                """,
                                (key, file_size, last_modified,),
                            )
                            row = cursor.fetchone()
                            connection.commit()
                            logger.info(f"Inserted: {bucket}:{key}")
        except Exception as e:
            logger.error(f"Failed file insert: {event}: {e}")
    elif event.get("source") and event["source"] == "aws.events":
        cronhandler(event, context)
    else:
        logger.warning(f"ingest-handler: nothing to do: {event}")


def getKeysFromSnsRecord(record):
    message = json.loads(record['Sns']['Message'])
    keys = []
    for _record in message['Records']:
        key = _record["s3"]["object"]["key"]
        logger.debug(f"key: {key}")
        keys.append({
            "bucket": _record["s3"]["bucket"]["name"],
            "key": key,
        })
    return keys


def getKeysFromS3Record(record):
    keys = [{
        "bucket": record["s3"]["bucket"]["name"],
        "key": record["s3"]["object"]["key"],
    }]
    return keys


def cronhandler(event, context):
    if settings.PAUSE_INGESTING:
        logger.info('Ingesting is paused')
        return None

    start_time = time()
    timeout = settings.INGEST_TIMEOUT  # manual timeout for testing
    ascending = settings.FETCH_ASCENDING if 'ascending' not in event else event['ascending']
    pipeline_limit = settings.PIPELINE_LIMIT if 'pipeline_limit' not in event else event['pipeline_limit']
    realtime_limit = settings.REALTIME_LIMIT if 'realtime_limit' not in event else event['realtime_limit']
    metadata_limit = settings.METADATA_LIMIT if 'metadata_limit' not in event else event['metadata_limit']

    logger.info(f"Running cron job: {event['source']}, ascending: {ascending}")

    # these exceptions are just a failsafe so that if something
    # unaccounted for happens we can still move on to the next
    # process. In case of this type of exception we will need to
    # fix it asap
    try:
        if metadata_limit > 0:
            cnt = 0
            loaded = 1
            while (
                    loaded > 0
                    and (time() - start_time) < timeout
            ):
                loaded = load_metadata_db(metadata_limit, ascending)
                cnt += loaded
                logger.info(
                    "loaded %s metadata records, timer: %0.4f",
                    cnt, time() - start_time
                )
    except Exception as e:
        logger.error(f"load metadata failed: {e}")

    try:
        if realtime_limit > 0:
            cnt = 0
            loaded = 1
            while (
                    loaded > 0
                    and (time() - start_time) < timeout
            ):
                loaded = load_db(realtime_limit, ascending)
                cnt += loaded
                logger.info(
                    "loaded %s fetch records, timer: %0.4f",
                    cnt, time() - start_time
                )
    except Exception as e:
        logger.error(f"load realtime failed: {e}")

    try:
        if pipeline_limit > 0:
            cnt = 0
            loaded = 1
            while (
                    loaded > 0
                    and (time() - start_time) < timeout
            ):
                loaded = load_measurements_db(pipeline_limit, ascending)
                cnt += loaded
                logger.info(
                    "loaded %s pipeline records, timer: %0.4f",
                    cnt, time() - start_time
                )
    except Exception as e:
        logger.error(f"load pipeline failed: {e}")

    logger.info("done processing: %0.4f seconds", time() - start_time)
