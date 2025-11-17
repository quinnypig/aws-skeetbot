from atproto import Client, client_utils, exceptions
import os
import boto3
from aws_lambda_powertools.utilities import parameters
import time
import feedparser
from aws_lambda_powertools import Logger, Metrics
from strip_tags import strip_tags
from atproto.exceptions import RequestException
from aws_lambda_powertools.metrics import MetricUnit


# Function to fetch environment variables with default values
def get_env_var(name, default):
    return os.environ.get(name, default)


# Custom exception for rate limit exceeded
class RateLimitExceededError(Exception):
    pass


# disable this if you don't want to enable degenerative AI summarization
cloudsplain_it = True


# This should be all the stuff you need to change if you want to customize this any
USERNAME_PARAM = os.environ.get(
    "SKEETBOT_USERNAME_PARAM", "/skeetbot/SKEETBOT_USERNAME"
)
PASSWORD_PARAM = os.environ.get(
    "SKEETBOT_PASSWORD_PARAM", "/skeetbot/SKEETBOT_PASSWORD"
)
ANTHROPIC_API_KEY_PARAM = os.environ.get(
    "ANTHROPIC_API_KEY_PARAM", "/skeetbot/ANTHROPIC_API_KEY"
)

RSS_FEED_URL = get_env_var("RSS_FEED_URL", "http://aws.amazon.com/new/feed/")
REGION = "us-west-2"

# Setting these up here so that they're only loaded once per function instantiation
ssm_provider = parameters.SSMProvider()
USERNAME = ssm_provider.get(USERNAME_PARAM, decrypt=True)
APP_PASSWORD = ssm_provider.get(PASSWORD_PARAM, decrypt=True)
ANTHROPIC_API_KEY = ssm_provider.get(ANTHROPIC_API_KEY_PARAM, decrypt=True)
posts_table = boto3.resource("dynamodb", region_name=REGION).Table(
    os.environ["PostsTableName"]
)
recency_threshold = int(os.environ["PostRecencyThreshold"])
logger = Logger()
client = Client()
client.login(USERNAME, APP_PASSWORD)
metrics = Metrics(namespace="SkeetBotMetrics")
# These will be initialized in the lambda handler to prevent accumulation across invocations
anthropic_counter = None
items = None


# Truncating mid-word feels unnatural, so we'll trim to the last word instead.
def trim_to_last_word(text, max_length):
    if len(text) <= max_length:
        return text
    trimmed = text[:max_length].rsplit(" ", 1)[0].rstrip(",")
    trimmed = trimmed + "…"
    return trimmed


# Check if the given time is within the specified number of minutes from now
def within(t: time.struct_time, minutes: int) -> bool:
    return abs(time.mktime(time.gmtime()) - time.mktime(t)) <= (minutes * 60)


# Check if the post with the given GUID has already been posted
def already_posted(guid: str) -> bool:
    return "Item" in posts_table.get_item(Key={"guid": guid})


if cloudsplain_it:
    import anthropic

    ai_client = anthropic.Anthropic(
        # defaults to os.environ.get("ANTHROPIC_API_KEY")
        api_key=ANTHROPIC_API_KEY,
    )

    # AWS is bad at explaining itself so we'll tag in AI to help.
    # We're using Anthropic directly intead of Bedrock because I
    # don't believe in rewarding bad behavior.
    def cloudsplain(text, trim: int):
        global anthropic_counter
        if anthropic_counter is not None:
            anthropic_counter += 1
        message = ai_client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1000,
            temperature=0,
            system="Do not announce what you are doing, simply do it. DO NOT LABEL IT AS A CLAUDE SUMMARY. If the supplied prompt is empty or contains garbage, return an empty set instead of a refusal.",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Summarize this text in {trim} characters or less:  \n {text}",
                        }
                    ],
                }
            ],
        )
        logger.info(f"Claude summarizing to {trim}: {message.content[0].text}")
        return message.content[0].text


# Post the entry to the client
def skeetit(entry, payload):
    logger.info(f"Posting {entry.guid} - {entry.title}")
    logger.info(f"Link length: {len(entry.link)}")
    text = (
        client_utils.TextBuilder()
        .link(entry.title, entry.link)
        .text("\n\n")
        .text(payload)
    )
    try:
        client.send_post(text)
    except RequestException as err:
        if err.response.status_code == 429:
            logger.error("Rate limit exceeded.")
            raise RateLimitExceededError("Rate limit exceeded.")
        logger.error(f"Failed to post {entry.guid} due to request exception: {err}")
        raise err
    except Exception as err:
        logger.error(f"Unexpected error while posting {entry.guid}: {err}")
        raise err
    return text


# Process each entry from the feed
def process_entry(entry):
    if within(entry.published_parsed, minutes=recency_threshold) and not already_posted(
        entry.guid
    ):
        logger.info(f"Processing {entry.guid} - {entry.title}")
        global items
        if items is not None:
            items += 1
        trim = 295 - len(entry.title)  # 300 max minus \n\n and …
        retry_count = 0
        max_retries = 5  # Limit retries to prevent excessive API calls
        while trim >= 100 and retry_count < max_retries:
            try:
                if cloudsplain_it:
                    payload = cloudsplain(entry.description, trim)
                else:
                    payload = trim_to_last_word(strip_tags(entry.description), trim)
                skeetit(entry, payload)
                posts_table.put_item(
                    Item={
                        "guid": entry.guid,
                        "title": entry.title,
                        "link": entry.link,
                    }
                )
                break
            except RateLimitExceededError:
                logger.error("Rate limit exceeded, stopping execution.")
                return False
            except exceptions.BadRequestError as err:
                logger.warning(f"Failed to post with length limit={trim}: {str(err)}")
                if err.response.status_code == 429:
                    logger.warning("Rate limited, backing off.")
                    break
                trim -= 15
                retry_count += 1
                if trim < 100 or retry_count >= max_retries:
                    logger.error(
                        f"Failed to post {entry.guid} after {retry_count} attempts."
                    )
        return True
    return True


# Lambda handler function
@metrics.log_metrics()
@logger.inject_lambda_context
def lambda_handler(event, context):
    global anthropic_counter, items
    # Initialize counters for each invocation
    anthropic_counter = 0
    items = 0
    for entry in feedparser.parse(RSS_FEED_URL).entries:
        if not process_entry(entry):
            break
    metrics.add_metric(
        name="AnthropicRequests", unit=MetricUnit.Count, value=anthropic_counter
    )
    metrics.add_metric(name="ItemsProcessed", unit=MetricUnit.Count, value=items)
