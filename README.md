### Setup

* create working dirs:
```
mkdir -p rawdata joined
```

* download raw dataset:
```
cd rawdata && wget https://synchronouscap.s3.eu-west-3.amazonaws.com/obchallenge/ETHUSDT-all.zip
```
* convert single zip archive to multiple small gz archives (partitioned by date)
```
# list all files             extract only file name            run bash shell for each file in parallel        unzip just selected file and compress it to gz
time unzip -l ETHSDT-all.zip | egrep '([0-9A-Za-z\\-]+.csv)' -o | xargs -P 8 -I{} bash -c 'echo processing {} && unzip -p ETHUSDT-all.zip {} | gzip > {}.gz
```
* build tool:
```
# run from repository root
cargo build --release
```
* parse archives:
```
ls rawdata/ | grep '\.csv.gz' | time xargs -P 8 -I{} bash -c 'echo processing {} && zcat rawdata/{} | target/release/parse-ethusd | gzip > joined/{}'
```
* inspect compressed:
```
ls -lah joined/

# output format are comma separated lines.
#   0    (type: i64)                                         1 (type: i32, 1 eth for X usd cents)                           2 (type: i32)
# unix time in microseconds (div by 1000000 to get seconds), eth-usd convesrion rate (cents, div by 100 to get dollars), unknown coeficent multiplied by 1000

zcat joined/ETHUSDT-2023-01-11.csv.gz | head -n 5

# output:
# 1673434298908000,133605,39132
# 1673434303910000,133605,107277
# 1673434308910000,133605,112778
# 1673434313912000,133605,152142
# 1673434318915000,133605,112471
# 1673434323919000,133605,52029
# 1673434328924000,133605,94480
# 1673434333926000,133605,31802

```