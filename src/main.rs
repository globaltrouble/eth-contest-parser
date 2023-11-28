use std::io::{self, BufRead, BufReader, BufWriter, Write};

// max line length:
// ls | grep gz | xargs -P 8 -I{} bash -c 'zcat {} | wc -L'
// < 30k, no lines > 30k chars

// read at least line (2*x line length), chose clothest val to pow of 2.
const READER_BUF_CAPACITY: usize = 65536;
const WRITER_BUF_CAPACITY: usize = 65536;
const VALUES_PER_CHUNK: usize = 2048;

fn main() {
    env_logger::builder().init();

    let stdin = io::stdin();
    let stdout = io::stdout();

    let mut reader = BufReader::with_capacity(READER_BUF_CAPACITY, stdin);
    let mut writer = BufWriter::with_capacity(WRITER_BUF_CAPACITY, stdout);

    let mut buf = String::with_capacity(READER_BUF_CAPACITY);
    let mut line: usize = 0;

    loop {
        buf.clear();
        match reader.read_line(&mut buf) {
            Ok(l) => {
                if l == 0 {
                    log::warn!("No data, finish reading");
                    break;
                }
                line += 1;

                let buf = buf.trim().trim_start_matches('\u{feff}');

                // command to perform research
                // ls . | grep '\.csv'| xargs -P 8 -I{} bash -c "zcat {} | python3 -c 'import sys; print(set(map(lambda row: len(row.split(\",\")), sys.stdin)))'" | tee > rows.txt
                // unix tm in ms, unknown constant 1000, unknown constant 1000, ~ 2k values of ethusd, ~2k values of

                let mut splt = buf.split(',');
                let tm = splt
                    .next()
                    .expect(&format!("No tm, str: `{:?}`, line: {:?}", &buf, line));
                let c1 = splt
                    .next()
                    .expect(&format!("No C1, str: `{:?}`, line: {:?}", &buf, line));
                let c2 = splt
                    .next()
                    .expect(&format!("No C2, str: `{:?}`, line: {:?}", &buf, line));

                let tm = tm
                    .parse::<i64>()
                    .expect(&format!("Can't parse tm, `{:?}`, line: {:?}", &buf, line));

                // convert tm to microseconds
                let tm = tm * 1000;

                let c1 = c1
                    .parse::<i64>()
                    .expect(&format!("Can't parse c1, `{:?}`, line: {:?}", &buf, line));
                let c2 = c2
                    .parse::<i64>()
                    .expect(&format!("Can't parse c2, `{:?}`, line: {:?}", &buf, line));

                if c1 != 1000 {
                    log::warn!("Unknown c1 coef: `{:?}`", c1);
                }

                if c2 != 1000 {
                    log::warn!("Unknown c2 coef: `{:?}`", c1);
                }

                const MAX_SPLIT_CAP: usize = VALUES_PER_CHUNK * 2;
                let mut parts = splt.collect::<heapless::Vec<&str, MAX_SPLIT_CAP>>();

                assert!(!parts.is_empty());

                if parts.last().expect("no last val").trim().is_empty() {
                    parts.pop();
                }

                assert!(!parts.is_empty());
                assert!(
                    parts.len() % 2 == 0,
                    "Line: {} values of unequal length, `{:?}`",
                    line,
                    &parts
                );

                // sample just first value
                // let count = parts.len() / 2;
                let count = 1;
                for eth_offset in 0..count {
                    let coef_offset = eth_offset + count;

                    let eth = parts[eth_offset].parse::<f32>().expect(&format!(
                        "Can't parse eth: line: {}, offset: {}",
                        line, eth_offset
                    ));

                    let coef = parts[coef_offset].parse::<f32>().expect(&format!(
                        "Can't parse eth: line: {}, offset: {}",
                        line, coef_offset
                    ));

                    let tm: i64 = tm + (eth_offset as i64) * 2500;

                    // use integer to simplify math
                    // eth has 2 decimal values after point
                    let eth = (eth * 100.0).round() as i32;
                    // coef has 3 decimal values after point
                    let coef = (coef * 1000.0).round() as i32;

                    writer
                        .write_fmt(format_args!("{},{},{}\n", tm, eth, coef))
                        .expect("Can't write csv row");
                }
            }
            Err(e) => {
                log::warn!("Can't read from stdin: {:?}", e);
                break;
            }
        }
    }

    writer.flush().expect("Can't flush writer");
}
