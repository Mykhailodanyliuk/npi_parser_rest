import asyncio
import json
import time

import aiohttp
import jellyfish
import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
}


async def get_page(session, url):
    while True:
        try:
            async with session.get(url, headers=headers) as r:
                print(r.status)
                if r.status == 200:
                    return await r.json()
        except asyncio.exceptions.TimeoutError:
            pass


async def get_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(get_page(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def get_all_data_urls(urls, limit=5000):
    timeout = aiohttp.ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=limit)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        data = await get_all(session, urls)
        return data


def upload_npi_data(npi_collection_rest_url):
    while True:
        sec_response = requests.get('https://www.sec.gov/files/company_tickers.json')
        if sec_response.status_code == 200:
            loc_json = json.loads(sec_response.text)
            break
        else:
            time.sleep(60)
    cik_title_list = [[loc_json[record].get('cik_str'), loc_json[record].get('title')] for record in loc_json]
    ticker_title_list = [[loc_json[record].get('ticker').replace("-", ""), loc_json[record].get('title')] for record in
                         loc_json]
    union_list = cik_title_list + ticker_title_list
    links = [
        f'https://api.orb-intelligence.com/3/search/?api_key=c66c5dad-395c-4ec6-afdf-7b78eb94166a&limit=10&cik={list_record[0]}'
        for list_record in cik_title_list]
    links.extend([
        f'https://api.orb-intelligence.com/3/search/?api_key=c66c5dad-395c-4ec6-afdf-7b78eb94166a&limit=10&ticker={list_record[0]}'
        for list_record in ticker_title_list])
    search_data = asyncio.run(get_all_data_urls(links, 2))
    links2 = []
    for index, data in enumerate(search_data):
        for results_data in data.get('results'):
            if jellyfish.jaro_winkler_similarity(results_data.get('name').lower(), union_list[index][1].lower()) > 0.85:
                links2.append(results_data.get('fetch_url'))
                break
    search_data2 = asyncio.run(get_all_data_urls(links2, 2))
    cik_npi_list = [[data.get('cik'), data.get('npis')] for data in search_data2 if data.get('npis') != []]
    some_list = []
    for cik, npi in cik_npi_list:
        data = {'cik': cik, 'npi': npi}
        print(data)
        some_list.append(data)
        response = requests.get(f'{npi_collection_rest_url}?cik={cik}')
        if response.status_code == 200:
            response_json = json.loads(response.text)
            is_in_database = False if response_json.get('total') == 0 else True
            if not is_in_database:
                requests.post(npi_collection_rest_url, json=data)


if __name__ == '__main__':
    while True:
        start_time = time.time()
        upload_npi_data('http://62.216.33.167:21005/api/npi_data')
        work_time = int(time.time() - start_time)
        time.sleep(abs(work_time % 14400 - 14400))
