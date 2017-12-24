# kuuki-heroku
kuuki's server deployed on heroku  
Powered by python3 & flask & mysql

## Required Add-ons
- ClearDB MySQL Ignite (free but need verifying account)
- Heroku Scheduler (free but need verifying account)

## Deployment
```
$ heroku create
$ heroku addons:create cleardb:ignite
$ git push heroku master
$ heroku run python manage.py init
```
Add command ```python manage.py runcrawler``` to scheduler dashboard  
Set frequency ```hourly```, next due ```:30```

(It is observed that official Silverlight publishing platform udpate its data around :28)

## Limitation
Limit 10k records for database  
But increasing 1495 (every station data) + 361 (every city data calculated by station data) records per hour

Compaction like that:
- Reserve last 1 hour station data in table 'raw' (1495 * 2, 1 for new increasing)
- Reserve last 12 hours city data in table 'work' (361 * 13, 1 for new increasing)
- Cities and stations information must be reserved (361 + 1495)

Total is 1495 * 3 + 361 * 14 = 9539 < 10000

## All APIs
1. List for available cities

```
GET /aqi/cities
```
2. Latest rank by aqi values *(Add '?reverse=1' for reverse order)*

```
GET /aqi/rank 
```
3. Latest detail for specified cities *(Split cities by ',' and no duplication)*

```
GET /aqi/latest?cities=110000,510100 
```
4. Last 1 to 12 hours detail for a certain city

```
GET /aqi/last4h?city=110000
```

## Reference
hebingchang / air-in-china [(https://github.com/hebingchang/air-in-china)](https://github.com/hebingchang/air-in-china)  
ernw / python-wcfbin [(https://github.com/ernw/python-wcfbin)](https://github.com/ernw/python-wcfbin)

## License
This project is under [the MIT License](https://mit-license.org/).