import logging
import requests
import sys
import operator
import os
import functools

from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool

import http.client as http

# status = sys.argv[1] if sys.argv[1] else 'INFO'

# http.HTTPConnection.debuglevel = 1

# logging.basicConfig()

# logger = logging.getLogger()
# logger.setLevel(getattr(logging, status))

# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(getattr(logging, status))
# requests_log.propagate = True


def login(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        payload = {
            'user.username': os.getenv('MED_USER'),
            'userSecurity.password': os.getenv('MED_PASS'),
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'
        }
        sess = requests.Session()
        sess.headers.update(headers)
        sess.post(
            "https://services.aamc.org/30/ssoLogin/home/login/process",
            data=payload
        )
        request = func(sess, *args, **kwargs)

        return request

    return wrapper


def scrape_parallel(start, end):

    msar = 'https://services.aamc.org/30/msar/schoolDetails/%d/about'
    pool = ThreadPool(8)

    links  = [(msar % i) for i in range(start, end)]

    # logger.info('Started from %s to %s' % (start, end))

    bucket = pool.map(requests.get, links)
    html   = [BeautifulSoup(req.content) for req in bucket]

    return [soup.find_all('div', id='schoolInfo') for soup in html]


@login
def get_df(session):

    req = session.post("https://services.aamc.org/30/msar/search/resultData", data={
        'limit': 1000
    }).json()['searchResults']['rows']

    df = pd.DataFrame(req)
    return df.set_index('key')


@login
def get_html(session, n, category='schoolInfo'):
    msar = 'https://services.aamc.org/30/msar/%s/%d/about' % (category, n)
    req  = session.get(msar)

    if not req.ok:
        req.raise_for_status()

    return BeautifulSoup(req.content)

def discover_schools(start, end):
    legit = []
    pool = ThreadPool(8)
    results = pool.map(get_html, range(start, end + 1))

    return [discard(i) for i in results]

def discard(result):
    if 'Sign In' in result.title.text:
        raise IOError('Not Logged In')
    if 'ERROR' in result.title.text:
        None
    else:
        return True

if __name__ == '__main__':
    schema = scrape_parallel(0,2)
    print(schema[0])
