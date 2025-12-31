use std::collections::HashSet;
use std::io;

use content_disposition::parse_content_disposition;
use futures::{TryFutureExt, TryStreamExt};
use opendal::{EntryMode, Error, ErrorKind, Metadata, Operator, options::ListOptions};
use typed_path::Utf8UnixPathBuf;

pub async fn copy(
    source: (Operator, String),
    destination: (Operator, String),
) -> Result<(), Error> {
    let (src_op, src_path) = source;
    let (dst_op, dst_path) = destination;

    Copier::new(src_op, dst_op).copy(src_path, dst_path).await
}

pub async fn copy_recursive(
    source: (Operator, String),
    destination: (Operator, String),
) -> Result<(), Error> {
    let (src_op, src_path) = source;
    let (dst_op, dst_path) = destination;

    Copier::new(src_op, dst_op)
        .copy_recursive(src_path, dst_path)
        .await
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
        self.copy_impl(source, destination, false).await
    }

    pub async fn copy_recursive(&self, source: String, destination: String) -> Result<(), Error> {
        self.copy_impl(source, destination, true).await
    }

    async fn copy_impl(
        &self,
        source: String,
        destination: String,
        recursive: bool,
    ) -> Result<(), Error> {
        let source = normalize_path(source);
        let destination = normalize_path(destination);

        let stat = self.source.stat(source.as_str()).await?;
        let source = Source::new(source, stat);

        match source.meta.mode() {
            EntryMode::DIR => self.copy_dir(source, destination, recursive).await,
            EntryMode::FILE => self.copy_file(source, destination).await,
            _ => Err(Error::new(ErrorKind::Unsupported, "Unknown entry mode")),
        }
    }

    async fn copy_dir(
        &self,
        source: Source,
        destination: Utf8UnixPathBuf,
        recursive: bool,
    ) -> Result<(), Error> {
        match self.destination.stat(destination.as_str()).await {
            Ok(stat) if stat.is_file() => {
                return Err(Error::new(
                    ErrorKind::NotADirectory,
                    "Cannot copy directory to a file",
                ));
            }
            Ok(_) => (), // Destination exists and is a directory, continue
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Destination doesn't exist, create it
                self.destination
                    .create_dir(&format!("{}/", destination))
                    .await?;
            }
            Err(e) => return Err(e),
        }

        // List files in source directory
        let options = if recursive {
            let mut opts = ListOptions::default();
            opts.recursive = true;
            Some(opts)
        } else {
            None
        };

        let mut lister = crate::list::lister(&self.source, source.path.as_str(), options).await?;

        // Track which directories we've already created to avoid duplicate create_dir calls
        let mut created_dirs = HashSet::new();

        // Mark the destination directory as already created
        created_dirs.insert(destination.clone());

        while let Some(entry) = lister.try_next().await? {
            if entry.metadata().is_dir() {
                continue;
            }

            let relative_path = Utf8UnixPathBuf::from(entry.path());
            let relative_path = relative_path
                .strip_prefix(source.path.clone())
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, err.to_string()).set_source(err)
                })?;
            let destination = destination.join(relative_path);

            if let Some(parent) = destination.parent()
                && !created_dirs.contains(&(parent.to_owned()))
            {
                self.destination.create_dir(&format!("{}/", parent)).await?;

                created_dirs.insert(parent.to_owned());
            }

            let source = Source::new(
                Utf8UnixPathBuf::from(entry.path()),
                entry.metadata().clone(),
            );

            self.do_copy_file(source, destination.as_str()).await?;
        }

        Ok(())
    }

    async fn copy_file(&self, source: Source, destination: Utf8UnixPathBuf) -> Result<(), Error> {
        let destination = match self.destination.stat(destination.as_str()).await {
            Ok(stat) if stat.is_dir() => destination.join(source.name()?), // Destination exists and is a directory
            Ok(_) => destination, // Destination exists and is a file (overwrite)
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Destination does not exist, ensure parent directory exists
                if let Some(parent) = destination.parent() {
                    self.destination.create_dir(parent.as_str()).await?;
                }

                destination
            }
            Err(e) => {
                return Err(e);
            }
        };

        self.do_copy_file(source, destination.as_str()).await
    }

    // Copy a file from one storage to another.
    // This function expects that the input parameters have been validated
    // (that is, each path points to a file).
    async fn do_copy_file(&self, source: Source, destination: &str) -> Result<(), Error> {
        let reader = self.source.reader(source.path.as_str()).await?;
        let mut writer_builder = self.destination.writer_with(destination);

        if let Some(content_type) = source.meta.content_type() {
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

#[derive(Debug, Clone)]
struct Source {
    path: Utf8UnixPathBuf,
    meta: Metadata,
}

impl Source {
    fn new(path: Utf8UnixPathBuf, meta: Metadata) -> Self {
        Self { path, meta }
    }

    fn name(&self) -> Result<String, Error> {
        self.path
            .file_name()
            .map(String::from)
            .or_else(|| {
                self.meta
                    .content_disposition()
                    .and_then(|cd| parse_content_disposition(cd).filename_full())
            })
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Source has no filename"))
    }
}

// - remove leading slash (root)
// - add trailing slash if directory
fn normalize_path(path: String) -> Utf8UnixPathBuf {
    let is_dir = path.ends_with('/');

    let mut path = Utf8UnixPathBuf::from(path)
        .normalize()
        .to_string()
        .trim_start_matches("/")
        .to_string();

    if is_dir {
        path.push('/');
    }

    Utf8UnixPathBuf::from(path)
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

    #[tokio::test]
    async fn test_copy_directory_non_recursive() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source directory structure
        source.write("path/file1.txt", "content1").await?;
        source.write("path/file2.txt", "content2").await?;
        source.write("path/subdir/file3.txt", "content3").await?;

        // Copy directory non-recursively
        copy(
            (source, "path/".to_string()),
            (destination.clone(), "other/".to_string()),
        )
        .await?;

        // Should copy only direct files, not subdirectories
        let buffer1 = destination.read("other/file1.txt").await.unwrap();
        assert_eq!(buffer1.to_vec(), "content1".as_bytes());

        let buffer2 = destination.read("other/file2.txt").await.unwrap();
        assert_eq!(buffer2.to_vec(), "content2".as_bytes());

        // file3.txt should not exist as it's in a subdirectory
        assert!(destination.read("other/file3.txt").await.is_err());
        assert!(destination.read("other/subdir/file3.txt").await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_directory_recursive() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source directory structure
        source.write("path/file1.txt", "content1").await?;
        source.write("path/file2.txt", "content2").await?;
        source.write("path/subdir/file3.txt", "content3").await?;
        source
            .write("path/subdir/nested/file4.txt", "content4")
            .await?;

        // Copy directory recursively
        copy_recursive(
            (source, "path/".to_string()),
            (destination.clone(), "other/".to_string()),
        )
        .await?;

        // Should copy all files preserving structure
        let buffer1 = destination.read("other/file1.txt").await.unwrap();
        assert_eq!(buffer1.to_vec(), "content1".as_bytes());

        let buffer2 = destination.read("other/file2.txt").await.unwrap();
        assert_eq!(buffer2.to_vec(), "content2".as_bytes());

        let buffer3 = destination.read("other/subdir/file3.txt").await.unwrap();
        assert_eq!(buffer3.to_vec(), "content3".as_bytes());

        let buffer4 = destination
            .read("other/subdir/nested/file4.txt")
            .await
            .unwrap();
        assert_eq!(buffer4.to_vec(), "content4".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_directory_to_existing_directory() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source files
        source.write("path/file1.txt", "content1").await?;
        source.write("path/file2.txt", "content2").await?;

        // Create existing destination directory
        destination.create_dir("existing/").await?;

        // Copy directory to existing directory
        copy(
            (source, "path/".to_string()),
            (destination.clone(), "existing/".to_string()),
        )
        .await?;

        let buffer1 = destination.read("existing/file1.txt").await.unwrap();
        assert_eq!(buffer1.to_vec(), "content1".as_bytes());

        let buffer2 = destination.read("existing/file2.txt").await.unwrap();
        assert_eq!(buffer2.to_vec(), "content2".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_directory_to_nonexistent_destination() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source files
        source.write("path/file1.txt", "content1").await?;
        source.write("path/file2.txt", "content2").await?;

        // Copy directory to non-existent destination (should create it)
        copy(
            (source, "path/".to_string()),
            (destination.clone(), "newdir/".to_string()),
        )
        .await?;

        let buffer1 = destination.read("newdir/file1.txt").await.unwrap();
        assert_eq!(buffer1.to_vec(), "content1".as_bytes());

        let buffer2 = destination.read("newdir/file2.txt").await.unwrap();
        assert_eq!(buffer2.to_vec(), "content2".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_directory_to_file_should_error() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source directory and files
        source.write("path/file1.txt", "content1").await?;

        // Create destination file
        destination.write("existing_file.txt", "existing").await?;

        // Attempting to copy directory to file should error
        let result = copy(
            (source, "path/".to_string()),
            (destination, "existing_file.txt".to_string()),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotADirectory);

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_empty_directory() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create empty source directory
        source.create_dir("empty/").await?;

        // Copy empty directory
        copy(
            (source, "empty/".to_string()),
            (destination.clone(), "new_empty/".to_string()),
        )
        .await?;

        // Check that destination directory exists (it should be created even if empty)
        let stat = destination.stat("new_empty/").await?;
        assert!(stat.is_dir());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_directory_with_nested_empty_dirs() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source structure with nested empty directories and files
        source.write("source/file.txt", "content").await?;
        source.create_dir("source/empty_dir/").await?;
        source
            .write("source/nested/deep/file.txt", "deep_content")
            .await?;

        // Copy directory recursively
        copy_recursive(
            (source, "source/".to_string()),
            (destination.clone(), "dest/".to_string()),
        )
        .await?;

        // Verify files are copied
        let buffer1 = destination.read("dest/file.txt").await.unwrap();
        assert_eq!(buffer1.to_vec(), "content".as_bytes());

        let buffer2 = destination.read("dest/nested/deep/file.txt").await.unwrap();
        assert_eq!(buffer2.to_vec(), "deep_content".as_bytes());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_directory_overwrite_existing_files() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source files
        source.write("source/file1.txt", "new_content1").await?;
        source.write("source/file2.txt", "new_content2").await?;

        // Create existing destination files with different content
        destination.create_dir("dest/").await?;
        destination.write("dest/file1.txt", "old_content1").await?;
        destination.write("dest/file2.txt", "old_content2").await?;

        // Copy directory (should overwrite existing files)
        copy(
            (source, "source/".to_string()),
            (destination.clone(), "dest/".to_string()),
        )
        .await?;

        // Verify files were overwritten
        let buffer1 = destination.read("dest/file1.txt").await.unwrap();
        assert_eq!(buffer1.to_vec(), "new_content1".as_bytes());

        let buffer2 = destination.read("dest/file2.txt").await.unwrap();
        assert_eq!(buffer2.to_vec(), "new_content2".as_bytes());

        Ok(())
    }

    #[test]
    fn test_normalize_path() {
        // Simple file paths
        assert_eq!(normalize_path("file.txt".to_string()).as_str(), "file.txt");

        // Directory paths
        assert_eq!(normalize_path("dir/".to_string()).as_str(), "dir/");

        // Nested paths
        assert_eq!(
            normalize_path("dir/subdir/file.txt".to_string()).as_str(),
            "dir/subdir/file.txt"
        );
        assert_eq!(
            normalize_path("dir/subdir/".to_string()).as_str(),
            "dir/subdir/"
        );

        // Leading slash removal
        assert_eq!(normalize_path("/file.txt".to_string()).as_str(), "file.txt");
        assert_eq!(normalize_path("/dir/".to_string()).as_str(), "dir/");
        assert_eq!(
            normalize_path("/dir/subdir/file.txt".to_string()).as_str(),
            "dir/subdir/file.txt"
        );

        // Dot segments
        assert_eq!(
            normalize_path("dir/./file.txt".to_string()).as_str(),
            "dir/file.txt"
        );
        assert_eq!(normalize_path("dir/./".to_string()).as_str(), "dir/");

        // Double dot segments
        assert_eq!(
            normalize_path("dir/subdir/../file.txt".to_string()).as_str(),
            "dir/file.txt"
        );
        assert_eq!(
            normalize_path("dir/subdir/../".to_string()).as_str(),
            "dir/"
        );

        // Multiple slashes
        assert_eq!(
            normalize_path("dir//subdir//file.txt".to_string()).as_str(),
            "dir/subdir/file.txt"
        );
        assert_eq!(
            normalize_path("dir//subdir//".to_string()).as_str(),
            "dir/subdir/"
        );

        // Complex cases
        assert_eq!(
            normalize_path("/dir/./subdir/../another//file.txt".to_string()).as_str(),
            "dir/another/file.txt"
        );
        assert_eq!(
            normalize_path("/dir/./subdir/../another//".to_string()).as_str(),
            "dir/another/"
        );

        // Edge cases
        assert_eq!(normalize_path("".to_string()).as_str(), "");
        assert_eq!(normalize_path("/".to_string()).as_str(), "/");
        assert_eq!(normalize_path(".".to_string()).as_str(), "");
        assert_eq!(normalize_path("./".to_string()).as_str(), "/");
        assert_eq!(normalize_path("..".to_string()).as_str(), "");
        assert_eq!(normalize_path("../".to_string()).as_str(), "/");
    }
}
