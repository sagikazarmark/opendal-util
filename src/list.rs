use futures::TryStreamExt;
use globset::Glob;
use opendal::{Entry, Error, ErrorKind, Operator, options::ListOptions};

use crate::glob;

pub async fn list(
    operator: &Operator,
    path: &str,
    options: Option<ListOptions>,
) -> Result<Vec<Entry>, Error> {
    if let Some(prefix) = glob::literal_prefix(path) {
        return list_glob(operator, prefix.as_str(), path, options).await;
    }

    let lister;

    if let Some(options) = options {
        lister = operator.lister_options(path, options).await?;
    } else {
        lister = operator.lister(path).await?;
    }

    let entries: Vec<Entry> = lister.try_collect().await?;

    Ok(entries)
}

pub async fn list_glob(
    operator: &Operator,
    prefix: &str,
    glob: &str,
    options: Option<ListOptions>,
) -> Result<Vec<Entry>, Error> {
    // Glob pattern needs recursive listing
    let mut options = options.unwrap_or_default();
    options.recursive = true;

    // Glob pattern needs recursive listing
    let glob = Glob::new(glob)
        .map_err(|err| Error::new(ErrorKind::Unexpected, "Invalid glob pattern").set_source(err))?
        .compile_matcher();

    let entries: Vec<Entry> = operator
        .lister_options(prefix, options)
        .await?
        .try_filter(|entry| {
            let matches = glob.is_match(entry.path());

            futures::future::ready(matches)
        })
        .try_collect()
        .await?;

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use opendal::services::Memory;

    use super::*;

    #[tokio::test]
    async fn test_list() -> Result<(), Error> {
        let builder = Memory::default();
        let operator = Operator::new(builder)?.finish();

        operator.write("path/to/file.txt", "").await?;
        operator.write("path/to/other/file.txt", "").await?;

        let entries = list(&operator, "path/to/", None).await?;

        let paths: Vec<_> = entries.iter().map(|e| e.path()).collect();
        assert_eq!(paths, vec!["path/to/file.txt", "path/to/other/"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_recursive() -> Result<(), Error> {
        let builder = Memory::default();
        let operator = Operator::new(builder)?.finish();

        operator.write("path/to/file.txt", "").await?;
        operator.write("path/to/other/file.txt", "").await?;

        let mut options = ListOptions::default();
        options.recursive = true;

        let entries = list(&operator, "path/to/", Some(options)).await?;

        let paths: Vec<_> = entries.iter().map(|e| e.path()).collect();
        assert_eq!(paths, vec!["path/to/file.txt", "path/to/other/file.txt"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_glob() -> Result<(), Error> {
        let builder = Memory::default();
        let operator = Operator::new(builder)?.finish();

        operator.write("path/to/file.txt", "").await?;
        operator.write("path/to/other/file.txt", "").await?;

        let entries = list(&operator, "path/**/file.txt", None).await?;

        let paths: Vec<_> = entries.iter().map(|e| e.path()).collect();
        assert_eq!(paths, vec!["path/to/file.txt", "path/to/other/file.txt"]);

        Ok(())
    }
}
