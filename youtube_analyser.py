import os
import sys
import logging
import requests
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in config file: {e.msg}", e.doc, e.pos)

def fetch_playlist_items_page(page_token=None):
    config = load_config('config.json')
    params = config["page_list_content"].get("params")
    if page_token is not None:
        params["pageToken"] = page_token
    url = config["page_list_content"].get("url")
    response = requests.get(url, params=params)
    payload = json.loads(response.text)
    return payload

def fetch_videos_page(video_id, page_token=None):
    config = load_config('config.json')
    params = config["video_content"].get("params")
    if page_token is not None:
        params["pageToken"] = page_token
    params["id"] = video_id
    url = config["video_content"].get("url")
    response = requests.get(url, params=params)
    payload = json.loads(response.text)
    return payload

def fetch_playlist_items(next_page_token=None):
    payload = fetch_playlist_items_page(next_page_token)
    yield from payload["items"]
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_playlist_items(next_page_token)

def fetch_videos(video_id, next_page_token=None):
    payload = fetch_videos_page(video_id, next_page_token)
    yield from payload["items"]
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_videos(video_id, next_page_token)

def summarize_video(video):
    return {
        "video_id": video["id"],
        "title": video["snippet"]["title"],
        "views": int(video["statistics"].get("viewCount", 0)),
        "likes": int(video["statistics"].get("likeCount", 0)),
        "comments": int(video["statistics"].get("commentCount", 0)),
    }

def get_serializing_producer():
    config = load_config('config.json')
    scheam_registry = config["schema_registry"]
    schema_registry_client = SchemaRegistryClient(scheam_registry)
    youtube_videos_value_schema = schema_registry_client.get_latest_version("youtube_videos-value")
    
    serializing_producer_config = config["kafka"]
    serializing_producer_config["key.serializer"] = StringSerializer()
    serializing_producer_config ["value.serializer"] = AvroSerializer(
                                                            schema_registry_client, 
                                                            youtube_videos_value_schema.schema.schema_str)
    serializing_producer = SerializingProducer(serializing_producer_config)
    return serializing_producer

def on_delivery(err, record):
    pass

def main():
    logger.info("START")
    producer = get_serializing_producer()
    for video_item in fetch_playlist_items():
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(video_id):
            logger.info("GOT %s", summarize_video(video))
            producer.produce(
                topic = "youtube_videos",
                key = video_id,
                value = {
                    "TITLE": video["snippet"]["title"],
                    "VIEWS": int(video["statistics"].get("viewCount", 0)),
                    "LIKES": int(video["statistics"].get("likeCount", 0)),
                    "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                },
                on_delivery = on_delivery

            )
    producer.flush()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())