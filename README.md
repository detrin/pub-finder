# Pub finder
Find the optimal place where to meet with your friends in Prague.

Try out the Web demo https://pub-finder.hermandaniel.com.

## How does it work?

We have 1400+ stops in Prague. Given the set of `k` stops (e.g. Krymská, Anděl, Muzeum) we would like to find a stop (`target_stop`) that is closest to each of the stops. What is definition of "closest"? Let's define for now the distance function between stop `A` and stop `B` as `dist(A, B)`. We can consider case 
1) where we minimize the maximum distance from `k` stops (e.g. `worst_case_dist = max(dist(target_stop, Krymská), dist(target_stop, Anděl), dist(target_stop, Muzeum))`)
2) or the case where we minimize the total distance from `k` stops to `target_stop` (e.g. `total_dist = dist(target_stop, Krymská) + dist(target_stop, Anděl) + dist(target_stop, Muzeum)`).

Now the simplest solution would be to use `dist()` function as true distance of the stops considering their GPS coordinates, but we can do better. Since the public transport has different speed of transport depending on the stops themselves I scraped almost all the combinations of the stops in both ways (around 2.1M). I scraped all with arrival date Friday 28.2.2025, 20:00. Now, the distance can be also in minutes that it takes to get from stop A to stop B.

We can now iterate over all stops in Prague and for given list of `k` stops we can calculate `worst_case_dist` or `total_dist` and we can take the best cases. What I am actually doing is that I select top `10` target stops based on geo distance and top `25` target stops based on time distance. Then for all of them I scrape the actual time it takes given the date and time and update the table based on that. I select top `15` target stops and that is the end result.

## Usage

### Local
```
uv venv --python=3.12
source .venv/bin/activate
uv pip install -r requirements.txt
uv run app.py
```
Now you can visit http://0.0.0.0:3000 and enjoy the app.

### Docker
```
docker build -t pub-finder-app .
docker run -p 3000:3000 --name pub-finder pub-finder-app
```
Now you can enjoy the app on http://localhost:3000. 

To remove the image
```
docker rm pub-finder
```


## Development
Prepare the geo data of stops scraped from PID.
```
python3.12 prepare_geo_data.py 
```

For scraping use the following. Repeat until you scrape all the combinations. Internally you can swith between IDOS and DPP providers. DPP has slightly higher error rate
```
uv run scraping.py --num-processes 50 --num-tasks 50
uv run manager.py --threshold-error-rate 0.1
jq '. | (length / 2138906) * 100' results.json
jq 'map(select(.error != "Failed to retrieve data."))' results.json > results_filtered.json; mv results_filtered.json results.json
```

## Sources
- https://idos.cz/vlakyautobusymhdvse/spojeni/
- https://pid.cz/zastavky-pid/zastavky-v-praze
- https://mapa.pid.cz/?filter=&zoom=12.0&lon=14.4269&lat=50.0874