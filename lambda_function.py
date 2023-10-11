import json
import requests
import feedparser
from bs4 import BeautifulSoup
import openai
from datetime import datetime, timedelta
import time
import random

# VARIABLES
openai.api_key = os.environ.get('openai_key', 'Default Value') # OPENAI

bucket_name = "19nt-news" # S3 target bucket for json files

time_period = 25 # max age in hours

rss_feed_urls = [
#   US / GLOBAL
    {"source": "Google Health", "url": "https://news.google.com/rss/topics/CAAqJQgKIh9DQkFTRVFvSUwyMHZNR3QwTlRFU0JXVnVMVWRDS0FBUAE?hl=en-US&gl=US&ceid=US%3Aen&oc=11"},
    {"source": "CNBC World", "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html"},
    {"source": "Healthshots", "url": "https://www.healthshots.com/rss-feeds/health-news/"},
    {"source": "FDA Food Safety", "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/food-safety-recalls/rss.xml"},
    {"source": "FDA Good Allergies", "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/food-allergies/rss.xml"},
    {"source": "USDA", "url": "https://www.usda.gov/rss/latest-releases.xml"},
#   EUROPE
    {"source": "EU:BBC Health", "url": "http://feeds.bbci.co.uk/news/health/rss.xml"},
    {"source": "EU:EFSA", "url": "https://www.efsa.europa.eu/en/press/rss"},
    {"source": "EU:UK Gov", "url": "https://www.food.gov.uk/rss-feed/alerts-food"},
#   CANADA
    {"source": "CA:CDC Food Safety", "url": "https://tools.cdc.gov/api/v2/resources/media/316422.rss"},
    {"source": "CA:CFIA Alerts", "url": "https://recalls-rappels.canada.ca/en/feed/cfia-alerts-recalls"},
    {"source": "CA:Globe & Mail", "url": "https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/canada/"},
    {"source": "CA:Global News", "url": "https://globalnews.ca/health/feed/"},
#   BRASIL
    {"source": "BR:Campo Grande", "url": "https://www.campograndenews.com.br/rss/rss.xml"},
    {"source": "BR:Rio Times", "url": "https://www.riotimesonline.com/feed/"},
    {"source": "BR:Brasil 247", "url": "https://www.brasil247.com/feed.rss"},
    {"source": "BR:Correio 24", "url": "https://www.correio24horas.com.br/rss"},
    {"source": "BR:Metropoles", "url": "https://www.metropoles.com/saude/feed"},
#   MEXICO
    {"source": "MX:Reforma", "url": "https://www.reforma.com/rss/portada.xml"},
    {"source": "MX:El Siglo de Torreón", "url": "https://www.elsiglodetorreon.com.mx/index.xml"},
    {"source": "MX:Mexico News Daily", "url": "https://mexiconewsdaily.com/feed/"},
    {"source": "MX: Vanguardia", "url": "https://vanguardia.com.mx/rss.xml"},
#   CHILE
    {"source": "CHILE:The Clinic", "url": "https://www.theclinic.cl/feed/"},
    {"source": "CHILE:El Rancagüino", "url": "https://www.elrancaguino.cl/feed/"},
    {"source": "CHILE:La Discusión", "url": "https://www.ladiscusion.cl/feed/"},
    {"source": "CHILE:La Nación", "url": "https://www.lanacion.cl/feed/"},
#   COLOMBIA
    {"source": "COL:La Nación", "url": "https://www.lanacion.com.co/feed/"},
    {"source": "COL:Diario Occidente", "url": "https://occidente.co/feed/"},
    {"source": "COL:The City Paper Bogotá", "url": "https://thecitypaperbogota.com/feed/"},
    {"source": "COL:The Bogotá Post", "url": "https://thebogotapost.com/feed/"},
    {"source": "COL:Minuto30", "url": "https://www.minuto30.com/feed/"},
    {"source": "COL:KienyKe", "url": "https://www.kienyke.com/feed"}
]


# FUNCTIONS

print('Loading function')

def send_email(subject, body, sender, recipients):
    import boto3
    ses_client = boto3.client('ses', region_name='us-east-1')  # Replace with your desired AWS region

    try:
        response = ses_client.send_email(
            Source=sender,
            Destination={'ToAddresses': recipients},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print("Email sent successfully! Message ID: " + response['MessageId'])
    except Exception as e:
        print("Error sending email: " + str(e))

def write_json_to_s3(bucket_name, file_name, data):
    import boto3
    import json
    from datetime import datetime

    # Initialize a session using Amazon S3
    s3 = boto3.client('s3')
    
    # Serialize the JSON data
    json_data = json.dumps(data)
    
    # Write the JSON data to S3
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data, ContentType='application/json')



def parse_date(published_date_str):
    # Manual conversion of some known timezones to their UTC offsets.
    timezone_mappings = {
        'EDT': '-0400',
        'EST': '-0500',
        'CST': '-0600',
        'PST': '-0800'
        # Add more mappings as needed
    }
    
    for tz, offset in timezone_mappings.items():
        published_date_str = published_date_str.replace(tz, offset)

    formats = ["%a, %d %b %Y %H:%M:%S %z","%a, %d %b %Y %H:%M:%S %Z"]
    
    for fmt in formats:
        try:
            return datetime.strptime(published_date_str, fmt)
        except ValueError:
            continue

    return None

def is_old(published_date_str):
    published_date = parse_date(published_date_str)
                    
    if published_date is not None:
        # Get the current time and date
        current_date = datetime.now(published_date.tzinfo)
        # Calculate the time difference
        time_difference = current_date - published_date
        # Check if it's more than 24 hours old
        if time_difference < timedelta(hours=time_period):
            return False
        else:
            return True

# Retry function
def retry_call(func, max_retries=5, backoff_factor=1, *args, **kwargs):
    for retry in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if retry == max_retries:
                raise
            sleep_time = backoff_factor * (2 ** retry)
            jitter = random.uniform(0, 0.1 * (2 ** retry))
            time.sleep(sleep_time + jitter)


def ai_sentiment(title_in):
    response = openai.ChatCompletion.create(
#        model="gpt-3.5-turbo",
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You will be provided with a news headline, and your task is to classify its sentiment as positive, neutral, or negative."},
            {"role": "user", "content": title_in}
        ],
        temperature=0,
        max_tokens=64,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    return response.choices[0].message['content'].strip()

def scrape_article_text(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    article_body = soup.find("article")
    if article_body is None:
        return None
    article_text = article_body.get_text(separator="\n")
    return article_text.strip()

def ai_summarize(article_in):
    scraped_text = scrape_article_text(article_in)
    if not scraped_text:
        return "ChatGPT: summary not possible"
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
#        model="gpt-4",
        messages=[
            {"role": "system", "content": "You will be provided with a news article, and your task is to summarize it in the English language."},
            {"role": "user", "content": scraped_text}
        ],
        temperature=0,
        max_tokens=250,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    return response.choices[0].message['content'].strip()


def ai_classify_bool(title_in):
    response = openai.ChatCompletion.create(
        model="gpt-4",
#        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You will be provided with a news headline. Your job is to classify the news headline, determining if it is related to outbreaks of any foodborne illness like E. Coli, Listeria and Salmonella. If it related to an outbreak, return 'True', otherwise return 'False'"},
            {"role": "user", "content": title_in}
        ],
        temperature=0,
        max_tokens=128,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    return response.choices[0].message['content'].strip()


def str_to_bool(s):
    return s.lower() == 'true'


def process_feeds(feed_urls):
    message = "This is your daily automated news feed\n\n"
    counter = 0
    pause_counter = 0
    pause_limit = 40

    message += "\n\n"

    for feed_url in feed_urls:
        feed = feedparser.parse(feed_url['url'])
        print("Processing feed:", feed_url['source'])
        if feed.status == 200:
            for entry in feed.entries:
                 
                if hasattr(entry, 'title') and hasattr(entry,'published') and hasattr(entry,'link'):
                
                    if not is_old(entry.published): # if the article is not too old
                        pause_counter = pause_counter +1
#                        if (pause_counter) % pause_limit == 0: # Pause periodically to prevent rate issues
#                            print("__Pausing ..")
#                            time.sleep(15)
                        # Retry the openai call if it hangs
                        time.sleep(0.4)
                        print("trying: ", entry.title)
                        maxretry = 5
                        for retry in range(1,maxretry +1):
                            try:
                                mytheme = ai_classify_bool(entry.title)
                                break
                            except Exception as e:
                                if retry == maxretry:
                                    raise
                            print("Pause and retry")
                            time.sleep(10)
                        mytheme = str_to_bool(mytheme)

                        if mytheme: # If we have a match
                            print("Match. ")
                            counter += 1
                            message += entry.title + "\n"
                            message += entry.published + "\n"
                            message += entry.link + "\n"
                            #message += "Sentiment: " + ai_sentiment(entry.title) + "\n\n"
                            summary = ai_summarize(entry.link)
                            message += summary
                            message += "\n________________________________\n\n"

                            #Prepare JSON data
                            data = {"datestamp": datetime.now().isoformat(),"topic": "Food Safety","title": entry.title,"source": feed_url['source'],"url": entry.link,"summary": summary}
                            file_name = "food/" + datetime.now().isoformat() + "-foodnews.json" # Keep this code here as there will be multiple writes in the for loop
                            print(file_name + "\n")
#                            write_json_to_s3(bucket_name, file_name, data)

                            
                else:
                    print("Missing fields:", feed.feed.title)
        else:
            print("Error fetching feed")

    if counter == 0:
        message += "No matching stories in last 24 hours\n"
        
    message += "FEEDS:\n" + ', '.join(feed['source'] for feed in rss_feed_urls)
    message += "\n\nKEYWORDS:\nUsing semantic search asking for outbreaks of foodborne illness, no keywords necessary\n\n"
    message += "\nEnd of message\n"
    print("End of run\n\n")
    return message, counter




def lambda_handler(event, context):
#    body = test_openai_installation()
    storycount = 0
    body, storycount = process_feeds(rss_feed_urls)
    subject = 'MyT-S Daily News, '+ str(storycount) + ' result(s)'
    sender = "harry@19nt.com"
    recipients = [
        "valmir@myt-s.com",
        "francine.shaw@myfoodsc.com",
#        "francine.shaw@myt-s.com",
        "matheus.deleo@myt-s.com",
        "chrismicallison@gmail.com"
        ]
    send_email(subject, body, sender, recipients)
    return body	


