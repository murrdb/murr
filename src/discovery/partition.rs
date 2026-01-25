use chrono::NaiveDate;
use object_store::path::Path as ObjectPath;

/// Represents a discovered date partition
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatePartition {
    pub date: NaiveDate,
    pub path: ObjectPath,
}

/// Parses a path segment as a YYYY-MM-DD date.
/// Returns None if the segment doesn't match the expected format.
pub fn parse_date_partition(segment: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(segment, "%Y-%m-%d").ok()
}

/// Extracts the date segment from a path relative to a prefix.
/// For path "prefix/2024-01-14/file.parquet" with prefix "prefix",
/// returns Some("2024-01-14").
fn extract_date_segment<'a>(path_str: &'a str, prefix: &str) -> Option<&'a str> {
    let relative = if prefix.is_empty() {
        path_str
    } else {
        let prefix_with_slash = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{}/", prefix)
        };
        path_str.strip_prefix(&prefix_with_slash)?
    };

    relative.split('/').next()
}

/// Filters and sorts object paths to find date partitions.
/// Returns partitions sorted in descending order (most recent first).
pub fn find_date_partitions(
    paths: impl IntoIterator<Item = ObjectPath>,
    prefix: &str,
) -> Vec<DatePartition> {
    let mut partitions: Vec<DatePartition> = paths
        .into_iter()
        .filter_map(|path| {
            let path_str = path.as_ref();
            let segment = extract_date_segment(path_str, prefix)?;
            let date = parse_date_partition(segment)?;

            // Build the partition path
            let partition_path = if prefix.is_empty() {
                ObjectPath::from(segment.to_string())
            } else {
                ObjectPath::from(format!("{}/{}", prefix.trim_end_matches('/'), segment))
            };

            Some(DatePartition {
                date,
                path: partition_path,
            })
        })
        .collect();

    // Deduplicate and sort descending
    partitions.sort_by(|a, b| b.date.cmp(&a.date));
    partitions.dedup_by(|a, b| a.date == b.date);

    partitions
}

/// Returns the path to the _SUCCESS marker for a partition.
pub fn success_marker_path(partition: &ObjectPath) -> ObjectPath {
    let path_str = partition.as_ref();
    ObjectPath::from(format!("{}/_SUCCESS", path_str.trim_end_matches('/')))
}

/// Filters paths to only .parquet files within a partition.
/// Excludes hidden files (those starting with _).
pub fn filter_parquet_files(
    paths: impl IntoIterator<Item = ObjectPath>,
    partition: &ObjectPath,
) -> Vec<ObjectPath> {
    let partition_prefix = format!("{}/", partition.as_ref().trim_end_matches('/'));

    paths
        .into_iter()
        .filter(|path| {
            let path_str = path.as_ref();
            if !path_str.starts_with(&partition_prefix) {
                return false;
            }
            if !path_str.ends_with(".parquet") {
                return false;
            }
            // Get the filename and check it's not hidden
            let filename = path_str.strip_prefix(&partition_prefix).unwrap_or(path_str);
            !filename.starts_with('_')
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date_partition_valid() {
        assert_eq!(
            parse_date_partition("2024-01-14"),
            Some(NaiveDate::from_ymd_opt(2024, 1, 14).unwrap())
        );
        assert_eq!(
            parse_date_partition("2023-12-31"),
            Some(NaiveDate::from_ymd_opt(2023, 12, 31).unwrap())
        );
    }

    #[test]
    fn test_parse_date_partition_invalid() {
        assert_eq!(parse_date_partition("not-a-date"), None);
        assert_eq!(parse_date_partition("20240114"), None);
        assert_eq!(parse_date_partition("2024/01/14"), None);
        assert_eq!(parse_date_partition(""), None);
    }

    #[test]
    fn test_find_date_partitions_sorts_descending() {
        let paths = vec![
            ObjectPath::from("prefix/2024-01-13/file.parquet"),
            ObjectPath::from("prefix/2024-01-15/file.parquet"),
            ObjectPath::from("prefix/2024-01-14/file.parquet"),
        ];

        let partitions = find_date_partitions(paths, "prefix");

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].date.to_string(), "2024-01-15");
        assert_eq!(partitions[1].date.to_string(), "2024-01-14");
        assert_eq!(partitions[2].date.to_string(), "2024-01-13");
    }

    #[test]
    fn test_find_date_partitions_deduplicates() {
        let paths = vec![
            ObjectPath::from("2024-01-14/file1.parquet"),
            ObjectPath::from("2024-01-14/file2.parquet"),
            ObjectPath::from("2024-01-14/_SUCCESS"),
        ];

        let partitions = find_date_partitions(paths, "");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].date.to_string(), "2024-01-14");
    }

    #[test]
    fn test_find_date_partitions_empty_prefix() {
        let paths = vec![
            ObjectPath::from("2024-01-14/file.parquet"),
            ObjectPath::from("2024-01-13/file.parquet"),
        ];

        let partitions = find_date_partitions(paths, "");

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].path.as_ref(), "2024-01-14");
        assert_eq!(partitions[1].path.as_ref(), "2024-01-13");
    }

    #[test]
    fn test_find_date_partitions_ignores_non_date_paths() {
        let paths = vec![
            ObjectPath::from("prefix/2024-01-14/file.parquet"),
            ObjectPath::from("prefix/random/file.parquet"),
            ObjectPath::from("prefix/not-a-date/file.parquet"),
        ];

        let partitions = find_date_partitions(paths, "prefix");

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].date.to_string(), "2024-01-14");
    }

    #[test]
    fn test_success_marker_path() {
        let partition = ObjectPath::from("prefix/2024-01-14");
        let marker = success_marker_path(&partition);
        assert_eq!(marker.as_ref(), "prefix/2024-01-14/_SUCCESS");
    }

    #[test]
    fn test_filter_parquet_files() {
        let partition = ObjectPath::from("prefix/2024-01-14");
        let paths = vec![
            ObjectPath::from("prefix/2024-01-14/part_0000.parquet"),
            ObjectPath::from("prefix/2024-01-14/part_0001.parquet"),
            ObjectPath::from("prefix/2024-01-14/_SUCCESS"),
            ObjectPath::from("prefix/2024-01-14/_metadata"),
            ObjectPath::from("prefix/2024-01-13/old.parquet"),
        ];

        let parquet_files = filter_parquet_files(paths, &partition);

        assert_eq!(parquet_files.len(), 2);
        assert!(
            parquet_files
                .iter()
                .all(|p| p.as_ref().ends_with(".parquet"))
        );
        assert!(
            parquet_files
                .iter()
                .all(|p| p.as_ref().starts_with("prefix/2024-01-14/"))
        );
    }

    #[test]
    fn test_filter_parquet_files_empty() {
        let partition = ObjectPath::from("prefix/2024-01-14");
        let paths = vec![
            ObjectPath::from("prefix/2024-01-14/_SUCCESS"),
            ObjectPath::from("prefix/2024-01-13/old.parquet"),
        ];

        let parquet_files = filter_parquet_files(paths, &partition);

        assert!(parquet_files.is_empty());
    }
}
