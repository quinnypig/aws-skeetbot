from atproto import Client, client_utils, exceptions
import os
import boto3
from aws_lambda_powertools.utilities import parameters
import time
import feedparser
from aws_lambda_powertools import Logger, Metrics
from datetime import datetime
from strip_tags import strip_tags
from atproto.exceptions import RequestException
from aws_lambda_powertools.metrics import MetricUnit


# Function to fetch environment variables with default values
def get_env_var(name, default):
    return os.environ.get(name, default)


# Custom exception for rate limit exceeded
class RateLimitExceededError(Exception):
    pass


# disable this if you don't want to enable snarky commentary
snarky_mode = True


# This should be all the stuff you need to change if you want to customize this any
USERNAME_PARAM = os.environ.get(
    "SNARKBOT_USERNAME_PARAM", "/snarkbot/bluesky-identifier"
)
PASSWORD_PARAM = os.environ.get(
    "SNARKBOT_PASSWORD_PARAM", "/snarkbot/bluesky-password"
)
ANTHROPIC_API_KEY_PARAM = os.environ.get(
    "ANTHROPIC_API_KEY", "/snarkbot/anthropic-api-key"
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
metrics = Metrics(namespace="snarkBotMetrics")
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
    try:
        response = posts_table.get_item(Key={"guid": guid})
        exists = "Item" in response
        logger.debug(f"DynamoDB check for {guid}: exists={exists}")
        return exists
    except Exception as e:
        logger.error(f"DynamoDB error checking {guid}: {e}")
        # Return True to avoid reprocessing on DB errors
        return True


if snarky_mode:
    import anthropic

    ai_client = anthropic.Anthropic(
        # defaults to os.environ.get("ANTHROPIC_API_KEY")
        api_key=ANTHROPIC_API_KEY,
    )

    # AWS is bad at explaining itself so we'll tag in AI to help.
    # We're using Anthropic directly intead of Bedrock because I
    # don't believe in rewarding bad behavior.
    def snarkify(text, trim: int):
        global anthropic_counter
        if anthropic_counter is not None:
            anthropic_counter += 1
        logger.info(f"Calling Anthropic API (count: {anthropic_counter}) with trim={trim}, text_length={len(text)}")
        message = ai_client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1000,
            temperature=0,
            system="You are Corey Quinn, a cloud economist known for extremely sarcastic, biting commentary about AWS. You're writing for BlueSky which has STRICT character limits - brevity is CRITICAL. Your responses must be punchy, concise snark about AWS's pricing, naming conventions, and corporate behavior. Be brutal, be funny, be accurate, but BE BRIEF. Mock their marketing speak and pricing complexity in the fewest words possible. Every character counts. DO NOT LABEL YOUR RESPONSE. If the prompt is empty, return an empty set.",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Transform this AWS announcement into a snarky BlueSky post. CRITICAL: You have EXACTLY {trim} characters maximum (BlueSky limit). Make every character count with brutal, concise snark about their naming, pricing, or corporate BS: \n {text}",
                        }
                    ],
                }
            ],
        )
        logger.info(f"Snarkified to {trim}: {message.content[0].text}")
        return message.content[0].text


# Post the entry to the client
def snarkit(entry, payload):
    logger.info(f"Posting {entry.guid} - {entry.title}")
    logger.info(f"Link: {entry.link}")
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
    is_recent = within(entry.published_parsed, minutes=recency_threshold)
    is_posted = already_posted(entry.guid)
    
    logger.info(f"Entry check - GUID: {entry.guid}, Recent: {is_recent}, Already posted: {is_posted}")
    
    if is_recent and not is_posted:
        logger.info(f"Processing new entry: {entry.guid} - {entry.title}")
        global items
        if items is not None:
            items += 1
        trim = 295 - len(entry.title)  # 300 max minus \n\n and …
        retry_count = 0
        max_retries = 5  # Limit retries to prevent excessive API calls
        while trim >= 100 and retry_count < max_retries:
            try:
                logger.info(f"Attempt {retry_count + 1}/{max_retries} for {entry.guid} with trim={trim}")
                if snarky_mode:
                    payload = snarkify(entry.description, trim)
                else:
                    payload = trim_to_last_word(strip_tags(entry.description), trim)
                snarkit(entry, payload)
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
                logger.warning(f"BadRequestError for {entry.guid} with trim={trim}: {str(err)}")
                logger.warning(f"Response status: {err.response.status_code}, Response body: {err.response.text if hasattr(err.response, 'text') else 'N/A'}")
                if err.response.status_code == 429:
                    logger.warning("Rate limited (429), stopping retries for this entry")
                    break
                trim -= 15
                retry_count += 1
                logger.info(f"Retrying with reduced trim={trim}")
                if trim < 100 or retry_count >= max_retries:
                    logger.error(
                        f"Failed to post {entry.guid} after {retry_count} attempts. Marking as failed to prevent retries."
                    )
                    # Mark as failed in DynamoDB to prevent endless retries
                    posts_table.put_item(
                        Item={
                            "guid": entry.guid,
                            "title": f"FAILED: {entry.title[:100]}",
                            "link": "FAILED_POST",
                            "error": str(err)[:500],
                            "timestamp": str(time.time())
                        }
                    )
                    metrics.add_metric(
                        name="FailedPosts", unit=MetricUnit.Count, value=1
                    )
        return True
    return True


# Circuit breaker: Track consecutive failures
CIRCUIT_BREAKER_TABLE = os.environ.get('CircuitBreakerTableName', None)
FAILURE_THRESHOLD = 5  # Number of consecutive failures before opening circuit

def check_circuit_breaker():
    """Check if circuit breaker is open"""
    if not CIRCUIT_BREAKER_TABLE:
        return False  # No circuit breaker table configured
    
    try:
        breaker_table = boto3.resource("dynamodb", region_name=REGION).Table(CIRCUIT_BREAKER_TABLE)
        response = breaker_table.get_item(Key={"id": "circuit_status"})
        if "Item" in response:
            status = response["Item"]
            if status.get("is_open", False):
                # Check if cooldown period has passed (5 minutes)
                open_time = float(status.get("open_time", 0))
                if time.time() - open_time < 300:  # 5 minutes
                    logger.warning("Circuit breaker is OPEN - skipping execution")
                    return True
                else:
                    # Reset circuit breaker
                    breaker_table.delete_item(Key={"id": "circuit_status"})
    except Exception as e:
        logger.error(f"Error checking circuit breaker: {e}")
    
    return False

def record_failure():
    """Record a failure and potentially open the circuit breaker"""
    if not CIRCUIT_BREAKER_TABLE:
        return
    
    try:
        breaker_table = boto3.resource("dynamodb", region_name=REGION).Table(CIRCUIT_BREAKER_TABLE)
        response = breaker_table.update_item(
            Key={"id": "failure_count"},
            UpdateExpression="ADD failure_count :inc",
            ExpressionAttributeValues={":inc": 1},
            ReturnValues="ALL_NEW"
        )
        
        failure_count = response["Attributes"].get("failure_count", 0)
        if failure_count >= FAILURE_THRESHOLD:
            # Open the circuit breaker
            breaker_table.put_item(
                Item={
                    "id": "circuit_status",
                    "is_open": True,
                    "open_time": time.time(),
                    "reason": f"Opened after {failure_count} consecutive failures"
                }
            )
            logger.error(f"Circuit breaker OPENED after {failure_count} failures")
            # Reset failure count
            breaker_table.delete_item(Key={"id": "failure_count"})
    except Exception as e:
        logger.error(f"Error recording failure: {e}")

def reset_failure_count():
    """Reset failure count on successful execution"""
    if not CIRCUIT_BREAKER_TABLE:
        return
    
    try:
        breaker_table = boto3.resource("dynamodb", region_name=REGION).Table(CIRCUIT_BREAKER_TABLE)
        breaker_table.delete_item(Key={"id": "failure_count"})
    except Exception as e:
        logger.error(f"Error resetting failure count: {e}")

# Lambda handler function
@metrics.log_metrics()
@logger.inject_lambda_context
def lambda_handler(event, context):
    global anthropic_counter, items
    
    # Check circuit breaker first
    if check_circuit_breaker():
        metrics.add_metric(
            name="CircuitBreakerOpen", unit=MetricUnit.Count, value=1
        )
        return {"statusCode": 200, "body": "Circuit breaker is open"}
    
    # Initialize counters for each invocation
    anthropic_counter = 0
    items = 0
    
    start_time = time.time()
    logger.info(f"Lambda started at {start_time} - Fetching RSS feed from {RSS_FEED_URL}")
    feed = feedparser.parse(RSS_FEED_URL)
    logger.info(f"RSS feed parsed - Found {len(feed.entries)} entries")
    
    for idx, entry in enumerate(feed.entries):
        logger.info(f"Processing entry {idx+1}/{len(feed.entries)}: {entry.guid} - {entry.title}")
        if not process_entry(entry):
            logger.warning(f"Stopping processing at entry {idx+1} due to rate limit")
            break
    elapsed_time = time.time() - start_time
    logger.info(f"Lambda completed in {elapsed_time:.2f}s - Anthropic calls: {anthropic_counter}, Items processed: {items}")
    metrics.add_metric(
        name="AnthropicRequests", unit=MetricUnit.Count, value=anthropic_counter
    )
    metrics.add_metric(name="ItemsProcessed", unit=MetricUnit.Count, value=items)
    
    # Alert if we're making too many API calls
    if anthropic_counter > 10:
        logger.warning(f"High API usage detected: {anthropic_counter} calls in single run")
        metrics.add_metric(
            name="HighAPIUsage", unit=MetricUnit.Count, value=1
        )
        record_failure()  # Record this as a failure
    else:
        reset_failure_count()  # Reset on successful execution
