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
#   0    (type: u64)        1 (type: f32, 1 eth for X usd)   2 (type: f32)
# unix time in miliseconds, eth-usd convesrion rate, unknown coeficent

zcat joined/ETHUSDT-2023-01-11.csv.gz | head -n 5

# output:
# 1673434200000,1336.05,39.132
# 1673434200000,1336.06,24.608
# 1673434200000,1336.07,27.029
# 1673434200000,1336.08,33.338
# 1673434200000,1336.09,44.132

```