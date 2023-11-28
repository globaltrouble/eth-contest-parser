use std::io::{self, BufRead, BufReader, BufWriter};
use std::sync::Arc;

use parquet::data_type::{Int32Type, Int64Type};
use parquet::{file::writer::SerializedFileWriter, schema::parser::parse_message_type};

// max line length:
// ls | grep gz | xargs -P 8 -I{} bash -c 'zcat {} | wc -L'
// < 30k, no lines > 30k chars

// read at least line (2*x line length), chose clothest val to pow of 2.
const READER_BUF_CAPACITY: usize = 65536;
const WRITER_BUF_CAPACITY: usize = 65536;
const VALUES_PER_CHUNK: usize = 2048;

const MSG_TYPE: &str = "
  message schema {
    REQUIRED INT64 tm;
    REQUIRED INT32 ethusd;
    REQUIRED INT32 coef;
  }
";

fn main() {
    env_logger::builder().init();

    let stdin = io::stdin();
    let stdout = io::stdout();

    let mut reader = BufReader::with_capacity(READER_BUF_CAPACITY, stdin);
    let writer = BufWriter::with_capacity(WRITER_BUF_CAPACITY, stdout);

    // example from https://docs.rs/parquet/latest/parquet/file/index.html
    let schema = Arc::new(parse_message_type(MSG_TYPE).expect("Can't parse schema"));
    let mut writer =
        SerializedFileWriter::new(writer, schema, Default::default()).expect("Can't create writer");

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

                let mut tms = heapless::Vec::<i64, VALUES_PER_CHUNK>::new();
                let mut eths = heapless::Vec::<i32, VALUES_PER_CHUNK>::new();
                let mut coefs = heapless::Vec::<i32, VALUES_PER_CHUNK>::new();

                let count = parts.len() / 2;
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

                    // use integer to simplify math
                    // eth has 2 decimal values after point
                    let eth = (eth * 100.0).round() as i32;
                    // coef has 3 decimal values after point
                    let coef = (coef * 1000.0).round() as i32;

                    let tm: i64 = tm + (eth_offset as i64) * 2500;
                    tms.push(tm).expect("Can't push tm");
                    eths.push(eth).expect("Can't push eth");
                    coefs.push(coef).expect("Can't push coef");
                }

                let mut row_group_writer = writer.next_row_group().expect("Can't get row group");
                // const COL_COUNT: i64 = 3;
                let mut tm_col = row_group_writer
                    .next_column()
                    .expect("can't get tm column")
                    .expect("no tm writer");

                tm_col
                    .typed::<Int64Type>()
                    .write_batch(&tms[..], None, None)
                    .expect("Can't write tm batch");
                tm_col.close().expect("can't close tm batch");

                let mut eth_col = row_group_writer
                    .next_column()
                    .expect("can't get eth column")
                    .expect("no eth writer");
                eth_col
                    .typed::<Int32Type>()
                    .write_batch(&eths[..], None, None)
                    .expect("Can't write eth batch");
                eth_col.close().expect("can't close eth batch");

                let mut coef_col = row_group_writer
                    .next_column()
                    .expect("can't get coef column")
                    .expect("no coef writer");
                coef_col
                    .typed::<Int32Type>()
                    .write_batch(&coefs[..], None, None)
                    .expect("Can't write coef batch");
                coef_col.close().expect("can't close coef batch");

                row_group_writer.close().expect("Can't close row group");
            }
            Err(e) => {
                log::warn!("Can't read from stdin: {:?}", e);
                break;
            }
        }
    }

    writer.close().expect("Can't close writer");
}
