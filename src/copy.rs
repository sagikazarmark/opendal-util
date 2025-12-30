use std::io;

use content_disposition::parse_content_disposition;
use futures::{TryFutureExt, TryStreamExt};
use opendal::{EntryMode, Error, ErrorKind, Metadata, Operator};
use typed_path::UnixPath;

pub async fn copy(
    source: (Operator, String),
    destination: (Operator, String),
) -> Result<(), Error> {
    let (src_op, src_path) = source;
    let (dst_op, dst_path) = destination;

    Copier::new(src_op, dst_op).copy(src_path, dst_path).await
}

pub struct Copier {
    source: Operator,
    destination: Operator,
}

impl Copier {
    pub fn new(source: Operator, destination: Operator) -> Self {
        Self {
            source,
            destination,
        }
    }

    pub async fn copy(&self, source: String, destination: String) -> Result<(), Error> {
        let stat = self.source.stat(source.as_str()).await?;

        match stat.mode() {
            // EntryMode::DIR => self.copy_dir(src_op, src_path, src_stat, dst_op, dst_path).await,
            EntryMode::FILE => self.copy_file(source, stat, destination).await,
            _ => Err(Error::new(ErrorKind::Unsupported, "Unknown entry mode")),
        }
    }

    async fn copy_file(
        &self,
        source: String,
        source_meta: Metadata,
        destination: String,
    ) -> Result<(), Error> {
        let destination = match self.destination.stat(destination.as_str()).await {
            Ok(stat) if stat.is_dir() => {
                UnixPath::new(destination.as_str()) // Destination exists and is a directory
                    .join(source_filename(source.as_str(), &source_meta)?)
                    .to_string_lossy()
                    .into_owned()
            }
            Ok(_) => destination.clone(), // Destination exists and is a file (overwrite)
            Err(e) if e.kind() == ErrorKind::NotFound => destination.clone(),
            Err(e) => {
                return Err(e);
            }
        };

        self.do_copy_file(source.as_str(), source_meta, destination.as_str())
            .await
    }

    // Copy a file from one storage to another.
    // This function expects that the input parameters have been validated
    // (that is, each path points to a file).
    async fn do_copy_file(
        &self,
        source: &str,
        source_meta: Metadata,
        destination: &str,
    ) -> Result<(), Error> {
        let reader = self.source.reader(source).await?;
        let mut writer_builder = self.destination.writer_with(destination);

        if let Some(content_type) = source_meta.content_type() {
            writer_builder = writer_builder.content_type(content_type);
        }
        // TODO: add other metadata?

        let mut writer = writer_builder.await?;

        let mut stream = reader.into_bytes_stream(..).await?;
        while let Some(chunk) = stream.try_next().map_err(|e| e.into_()).await? {
            writer.write(chunk).await?;
        }

        writer.close().await?;

        Ok(())
    }
}

fn source_filename(path: &str, meta: &Metadata) -> Result<String, Error> {
    UnixPath::new(path)
        .file_name()
        .map(|name| String::from_utf8_lossy(name).into_owned())
        .or_else(|| {
            meta.content_disposition()
                .and_then(|cd| parse_content_disposition(cd).filename_full())
        })
        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Source has no filename."))
}

pub trait IntoErrorExt {
    fn into_(self) -> Error;
}

impl IntoErrorExt for io::Error {
    fn into_(self) -> Error {
        let kind = match self.kind() {
            io::ErrorKind::NotFound => ErrorKind::NotFound,
            io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
            io::ErrorKind::IsADirectory => ErrorKind::IsADirectory,
            io::ErrorKind::NotADirectory => ErrorKind::NotADirectory,
            _ => ErrorKind::Unexpected,
        };

        Error::new(kind, self.to_string()).set_source(self)
    }
}

#[cfg(test)]
mod tests {
    use opendal::services::Memory;

    use super::*;

    #[tokio::test]
    async fn test_copy_file() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        source.write("path/to/file.txt", "foo").await?;

        copy(
            (source, "path/to/file.txt".to_string()),
            (destination.clone(), "".to_string()),
        )
        .await?;

        let buffer = destination.read("file.txt").await.unwrap();
        assert_eq!(buffer.to_vec(), "foo".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_file_overwrite() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        source.write("path/to/file.txt", "foo").await?;
        destination.write("file.txt", "bar").await?;

        copy(
            (source, "path/to/file.txt".to_string()),
            (destination.clone(), "file.txt".to_string()),
        )
        .await?;

        let buffer = destination.read("file.txt").await.unwrap();
        assert_eq!(buffer.to_vec(), "foo".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_file_to_directory() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        source.write("path/to/file.txt", "foo").await?;
        destination.create_dir("path/").await?;

        copy(
            (source, "path/to/file.txt".to_string()),
            (destination.clone(), "path/".to_string()),
        )
        .await?;

        let buffer = destination.read("path/file.txt").await.unwrap();
        assert_eq!(buffer.to_vec(), "foo".as_bytes());

        Ok(())
    }
}
