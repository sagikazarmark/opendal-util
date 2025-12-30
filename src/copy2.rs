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

    let src_stat = src_op.stat(src_path.as_str()).await?;

    match src_stat.mode() {
        EntryMode::DIR => copy_dir(src_op, src_path, src_stat, dst_op, dst_path).await,
        EntryMode::FILE => copy_file(src_op, src_path, src_stat, dst_op, dst_path).await,
        _ => {
            return Err(Error::new(ErrorKind::Unsupported, "Unknown entry mode"));
        }
    }
}

async fn copy_dir(
    src_op: Operator,
    src_path: String,
    src_stat: Metadata,
    dst_op: Operator,
    dst_path: String,
) -> Result<(), Error> {
    let real_dst_path = match dst_op.stat(&dst_path).await {
        Ok(stat) if stat.is_dir() => {
            // Destination exists and is a directory
            if let Some(filename) = UnixPath::new(&src_path).file_name() {
                UnixPath::new(&dst_path)
                    .join(filename)
                    .to_string_lossy()
                    .to_string()
            } else if let Some(filename) = src_stat
                .content_disposition()
                .and_then(|cd| parse_content_disposition(cd).filename_full())
            {
                filename
            } else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Cannot copy source '{}' into directory '{}': Source has no filename.",
                        src_path, dst_path
                    ),
                ));
            }
        }
        Ok(_) => {
            // Destination exists and is a file (overwrite)
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Directory copy destination '{}' exists and is a file.",
                    dst_path
                ),
            ));
        }
        Err(e) if e.kind() == ErrorKind::NotFound => dst_path.clone(),
        Err(e) => {
            return Err(e);
        }
    };

    do_copy_file(
        &src_op,
        src_path.as_str(),
        src_stat,
        &dst_op,
        real_dst_path.as_str(),
    )
    .await
}

async fn copy_file(
    src_op: Operator,
    src_path: String,
    src_stat: Metadata,
    dst_op: Operator,
    dst_path: String,
) -> Result<(), Error> {
    let real_dst_path = match dst_op.stat(&dst_path).await {
        Ok(stat) if stat.is_dir() => {
            // Destination exists and is a directory
            if let Some(filename) = UnixPath::new(&src_path).file_name() {
                UnixPath::new(&dst_path)
                    .join(filename)
                    .to_string_lossy()
                    .to_string()
            } else if let Some(filename) = src_stat
                .content_disposition()
                .and_then(|cd| parse_content_disposition(cd).filename_full())
            {
                filename
            } else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Cannot copy source '{}' into directory '{}': Source has no filename.",
                        src_path, dst_path
                    ),
                ));
            }
        }
        Ok(_) => {
            // Destination exists and is a file (overwrite)
            dst_path.clone()
        }
        Err(e) if e.kind() == ErrorKind::NotFound => dst_path.clone(),
        Err(e) => {
            return Err(e);
        }
    };

    do_copy_file(
        &src_op,
        src_path.as_str(),
        src_stat,
        &dst_op,
        real_dst_path.as_str(),
    )
    .await
}

// Copy a file from one storage to another.
// This function expects that the input parameters have been validated
// (that is, each path points to a file).
async fn do_copy_file(
    src_op: &Operator,
    src_path: &str,
    src_stat: Metadata,
    dst_op: &Operator,
    dst_path: &str,
) -> Result<(), Error> {
    let reader = src_op.reader(src_path).await?;
    let mut writer_builder = dst_op.writer_with(dst_path);

    if let Some(content_type) = src_stat.content_type() {
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

pub struct Copier {
    pub source: Operator,
    pub destination: Operator,
}

impl Copier {
    pub async fn copy(&self, source: String, destination: String) -> Result<(), Error> {
        let meta = self.source.stat(source.as_str()).await?;

        match meta.mode() {
            // EntryMode::DIR => copy_dir(src_op, src_path, src_stat, dst_op, dst_path).await,
            EntryMode::FILE => self.copy_file(source, meta, destination).await,
            _ => {
                return Err(Error::new(ErrorKind::Unsupported, "Unknown entry mode"));
            }
        }
    }

    async fn copy_file(
        &self,
        source: String,
        source_meta: Metadata,
        destination: String,
    ) -> Result<(), Error> {
        let real_dst_path = match self.destination.stat(&destination).await {
            Ok(stat) if stat.is_dir() => {
                UnixPath::new(&destination) // Destination exists and is a directory
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

        self.do_copy_file(source.as_str(), source_meta, real_dst_path.as_str())
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
