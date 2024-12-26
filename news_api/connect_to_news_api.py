import requests
import time
from datetime import datetime
from groq import Groq
from pymongo import MongoClient

NEWSAPI_KEY = "badc9499-8751-4840-93d5-6276f22d9002"
GROQ_API_KEY = "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"
OPENCAGE_API_KEY = "462e5cd9cb644aac8a43f85673006449"


def get_news(page=1):
    url = "https://eventregistry.org/api/v1/article/getArticles"
    data = {
        "action": "getArticles",
        "keyword": "terror attack",
        "keywordLoc": "body,title",
        "lang": ["eng"],
        "articlesPage": page,
        "articlesCount": 100,
        "articlesSortBy": "date",
        "articlesSortByAsc": False,
        "resultType": "articles",
        "apiKey": NEWSAPI_KEY,
        "forceMaxDataTimeWindow": 31
    }

    r = requests.post(url, json=data)
    if r.status_code == 200:
        return r.json()['articles']['results']
    return []


def check_if_terror(text):
    client = Groq(api_key=GROQ_API_KEY)
    prompt = f"Classify this news text into: 1. General News 2. Historical Terror Event 3. Current Terror Event Text: {text} Return only the number (1, 2, or 3)."
    resp = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="mixtral-8x7b-32768"
    )
    return resp.choices[0].message.content


def get_place(text):
    client = Groq(api_key=GROQ_API_KEY)
    prompt = f"Extract location (city, country) from: {text} Return only: city, country"
    resp = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="mixtral-8x7b-32768"
    )
    return resp.choices[0].message.content


def get_location_coords(place):
    if place:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={place}&key={OPENCAGE_API_KEY}"
        r = requests.get(url)
        data = r.json()
        if len(data['results']) > 0:
            loc = data['results'][0]['geometry']
            return loc['lat'], loc['lng']
    return None, None


client = MongoClient('mongodb://localhost:27017/')
db = client['terror_news']
collection = db['real_time_news']


def main():
    page = 1
    while True:
        print(f"Getting page {page}")
        articles = get_news(page)

        if not articles:
            time.sleep(2)
            page = page + 1
            if page > 5:
                page = 1
            continue

        for article in articles:
            category = check_if_terror(article['body'])

            print("\nTitle:", article['title'])
            print("Category:", category)

            if category in ['2', '3']:
                place = get_place(article['body'])
                print("Location found:", place)
                lat, lng = get_location_coords(place)

                data = {
                    'title': article['title'],
                    'body': article['body'],
                    'category': category,
                    'location': place,
                    'coordinates': {'lat': lat, 'lng': lng},
                    'timestamp': datetime.now()
                }
                collection.insert_one(data)
                print("Saved to database!")

        page = page + 1
        time.sleep(2)


if __name__ == '__main__':
    print("News processing with News API and GroQ...")
    main()

