import requests

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
headers = {
    'X-Nickname': 'Anastasia',
    'X-Cohort': '13',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}
params = {
    'sort_field': 'id',
    'sort_direction': 'asc',
    'limit': 50,
    'offset': 0
}

response = requests.get(url, headers=headers, params=params)
data = response.json()
result = str(data)
print(result)