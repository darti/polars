use std::io::{self, Cursor, Read, Seek};
use std::path::PathBuf;
use std::sync::OnceLock;

use opendal::Operator;

use crate::mmap::MmapBytesReader;

pub struct OpendalReader {
    operator: Operator,
    path: PathBuf,
    bytes: OnceLock<Option<Cursor<Vec<u8>>>>,
}

impl OpendalReader {
    pub fn new(operator: Operator, path: PathBuf) -> Self {
        Self {
            operator,
            path,
            bytes: OnceLock::new(),
        }
    }

    fn get_bytes_mut(&mut self) -> &mut Option<Cursor<Vec<u8>>> {
        let _ = self.get_bytes();

        self.bytes.get_mut().unwrap()
    }

    fn get_bytes(&self) -> &Option<Cursor<Vec<u8>>> {
        self.bytes.get_or_init(|| {
            self.path
                .to_str()
                .and_then(|s| self.operator.blocking().read(s).ok())
                .map(|b| Cursor::new(b.to_vec()))
        })
    }
}

impl MmapBytesReader for OpendalReader {
    fn to_bytes(&self) -> Option<&[u8]> {
        let cursor = self.get_bytes();

        cursor.as_ref().map(|c| c.get_ref().as_ref())
    }
}

impl Read for OpendalReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let cursor = self.get_bytes_mut();

        cursor.as_mut().unwrap().read(buf)
    }
}

impl Seek for OpendalReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let cursor = self.get_bytes_mut();

        cursor.as_mut().unwrap().seek(pos)
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use indoc::indoc;
    use opendal::services::Memory;
    use opendal::Operator;
    use polars_core::assert_df_eq;
    use polars_core::frame::DataFrame;
    use polars_core::prelude::*;
    use polars_core::series::Series;

    use crate::csv::read::CsvReadOptions;
    use crate::SerReader;

    #[test]
    #[cfg(feature = "csv")]
    fn test_csv() {
        let data = indoc! {"
            col1,col2
            a,c
            b,d
        "};

        let col1 = Series::new("col1", ["a", "b"].as_ref());
        let col2 = Series::new("col2", ["c", "d"].as_ref());

        let df_ref = DataFrame::new(vec![col1, col2]).unwrap();

        let builder = Memory::default();
        let op: Operator = Operator::new(builder).unwrap().finish();

        op.blocking().write("test.csv", data).unwrap();

        let reader = CsvReadOptions::default()
            .with_has_header(true)
            .into_reader_with_file_handle(super::OpendalReader::new(op, "test.csv".into()));

        let df = reader.finish().unwrap();

        assert_df_eq!(df, df_ref);
    }

    #[test]
    #[cfg(feature = "json")]
    fn test_json() {
        use indoc::indoc;

        use crate::json::JsonReader;

        let data = indoc! {r#"
        [
            {"col1": "a", "col2": "c"},
            {"col1": "b", "col2": "d"}
        ]
        "#};

        let df_ref = JsonReader::new(Cursor::new(data)).finish().unwrap();

        let builder = Memory::default();
        let op: Operator = Operator::new(builder).unwrap().finish();

        op.blocking().write("test.json", data).unwrap();

        let reader = JsonReader::new(super::OpendalReader::new(op, "test.json".into()));

        let df = reader.finish().unwrap();

        assert_df_eq!(df, df_ref);
    }
}
