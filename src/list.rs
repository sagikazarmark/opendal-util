use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use globset::Glob;
use opendal::{Entry, Error, ErrorKind, Operator, options::ListOptions};

use crate::glob;

pub async fn list(
    operator: &Operator,
    path: &str,
    options: Option<ListOptions>,
) -> Result<Vec<Entry>, Error> {
    let entries: Vec<Entry> = lister(operator, path, options).await?.try_collect().await?;

    Ok(entries)
}

pub async fn lister(
    operator: &Operator,
    path: &str,
    options: Option<ListOptions>,
) -> Result<BoxStream<'static, Result<Entry, Error>>, Error> {
    if let Some(prefix) = glob::literal_prefix(path) {
        // Glob pattern needs recursive listing
        let mut options = options.unwrap_or_default();
        options.recursive = true;

        let glob = Glob::new(path)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Invalid glob pattern").set_source(err)
            })?
            .compile_matcher();

        return Ok(operator
            .lister_options(prefix.as_str(), options)
            .await?
            .try_filter(move |entry| {
                let matches = glob.is_match(entry.path());

                futures::future::ready(matches)
            })
            .boxed());
    }

    if let Some(options) = options {
        return Ok(operator.lister_options(path, options).await?.boxed());
    }

    Ok(operator.lister(path).await?.boxed())
}

pub async fn glob_lister(
    operator: &Operator,
    prefix: &str,
    glob: &str,
    options: Option<ListOptions>,
) -> Result<BoxStream<'static, Result<Entry, Error>>, Error> {
    // Glob pattern needs recursive listing
    let mut options = options.unwrap_or_default();
    options.recursive = true;

    // Glob pattern needs recursive listing
    let glob = Glob::new(glob)
        .map_err(|err| Error::new(ErrorKind::Unexpected, "Invalid glob pattern").set_source(err))?
        .compile_matcher();

    Ok(operator
        .lister_options(prefix, options)
        .await?
        .try_filter(move |entry| {
            let matches = glob.is_match(entry.path());

            futures::future::ready(matches)
        })
        .boxed())
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
