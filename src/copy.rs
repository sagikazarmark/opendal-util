use std::collections::HashSet;
use std::io;

use content_disposition::parse_content_disposition;
use futures::{TryFutureExt, TryStreamExt};
use opendal::{EntryMode, Error, ErrorKind, Metadata, Operator, options::ListOptions};
use typed_path::Utf8UnixPathBuf;

use crate::{glob, list};

pub async fn copy(
    source: (Operator, impl Into<String>),
    destination: (Operator, impl Into<String>),
) -> Result<(), Error> {
    let (src_op, src_path) = source;
    let (dst_op, dst_path) = destination;

    Copier::new(src_op, dst_op).copy(src_path, dst_path).await
}

pub struct Copier {
    source: Operator,
    destination: Operator,
}

/// Options for controlling copy behavior.
#[derive(Debug, Copy, Clone, Default)]
pub struct CopyOptions {
    /// Whether to copy directories recursively.
    ///
    /// When `true`, all files and subdirectories within a directory will be copied.
    /// When `false`, only the immediate contents of the directory are copied.
    pub recursive: bool,
}

impl Copier {
    pub fn new(source: Operator, destination: Operator) -> Self {
        Self {
            source,
            destination,
        }
    }

    pub async fn copy(
        &self,
        source: impl Into<String>,
        destination: impl Into<String>,
    ) -> Result<(), Error> {
        self.copy_options(source, destination, CopyOptions::default())
            .await
    }

    pub async fn copy_options(
        &self,
        source: impl Into<String>,
        destination: impl Into<String>,
        options: CopyOptions,
    ) -> Result<(), Error> {
        let source = source.into();
        let destination = destination.into();

        let source = normalize_path(&source);
        let destination = normalize_path(&destination);

        // Check if source contains glob patterns
        if glob::has_glob_chars(source.as_str()) {
            return self.copy_glob(source, destination).await;
        }

        let stat = self.source.stat(source.as_str()).await?;
        let source = Source::new(source, stat);

        match source.meta.mode() {
            EntryMode::DIR => self.copy_dir(source, destination, options.recursive).await,
            EntryMode::FILE => self.copy_file(source, destination).await,
            _ => Err(Error::new(ErrorKind::Unsupported, "Unknown entry mode")),
        }
    }

    async fn copy_glob(
        &self,
        source: Utf8UnixPathBuf,
        destination: Utf8UnixPathBuf,
    ) -> Result<(), Error> {
        // Get the literal prefix to determine the base path for relative path computation
        let prefix = glob::extract_glob_prefix(source.as_str()).unwrap_or_default();
        let prefix = Utf8UnixPathBuf::from(prefix);

        let lister = crate::list::lister(&self.source, source.as_str(), None).await?;

        self.copy_entries(lister, prefix, destination).await
    }

    async fn copy_dir(
        &self,
        source: Source,
        destination: Utf8UnixPathBuf,
        recursive: bool,
    ) -> Result<(), Error> {
        let options = if recursive {
            Some(ListOptions {
                recursive: true,
                ..Default::default()
            })
        } else {
            None
        };

        let lister = list::lister(&self.source, source.path.as_str(), options).await?;

        self.copy_entries(lister, source.path, destination).await
    }

    async fn copy_entries(
        &self,
        mut lister: futures::stream::BoxStream<'static, Result<opendal::Entry, Error>>,
        source_prefix: Utf8UnixPathBuf,
        destination: Utf8UnixPathBuf,
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

        // Track which directories we've already created to avoid duplicate create_dir calls
        let mut created_dirs = HashSet::new();

        // Mark the destination directory as already created
        created_dirs.insert(destination.clone());

        while let Some(entry) = lister.try_next().await? {
            if entry.metadata().is_dir() {
                continue;
            }

            let entry_path = Utf8UnixPathBuf::from(entry.path());

            // Compute relative path from the source prefix
            let relative_path = if source_prefix.as_str().is_empty() {
                entry_path.clone()
            } else {
                entry_path
                    .strip_prefix(&source_prefix)
                    .map(|p| Utf8UnixPathBuf::from(p.to_string().trim_start_matches('/')))
                    .unwrap_or_else(|_| entry_path.clone())
            };

            let dest_path = destination.join(&relative_path);

            if let Some(parent) = dest_path.parent()
                && !created_dirs.contains(&(parent.to_owned()))
            {
                self.destination.create_dir(&format!("{}/", parent)).await?;

                created_dirs.insert(parent.to_owned());
            }

            let source = Source::new(entry_path, entry.metadata().clone());

            self.do_copy_file(source, dest_path.as_str()).await?;
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
        while let Some(chunk) = stream
            .try_next()
            .map_err(IoErrorExt::into_opendal_error)
            .await?
        {
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

/// Normalizes a path by:
/// - Removing leading slashes (root)
/// - Preserving trailing slashes for directories
/// - Resolving `.` and `..` components
fn normalize_path(path: &str) -> Utf8UnixPathBuf {
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

trait IoErrorExt {
    fn into_opendal_error(self) -> Error;
}

impl IoErrorExt for io::Error {
    fn into_opendal_error(self) -> Error {
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

        let copier = Copier::new(source.clone(), destination.clone());

        // Copy directory recursively
        copier
            .copy_options(
                "path/",
                "other/",
                CopyOptions {
                    recursive: true,
                    ..Default::default()
                },
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

        let copier = Copier::new(source.clone(), destination.clone());

        // Copy directory recursively
        copier
            .copy_options(
                "source/",
                "dest/",
                CopyOptions {
                    recursive: true,
                    ..Default::default()
                },
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
        assert_eq!(normalize_path("file.txt").as_str(), "file.txt");

        // Directory paths
        assert_eq!(normalize_path("dir/").as_str(), "dir/");

        // Nested paths
        assert_eq!(
            normalize_path("dir/subdir/file.txt").as_str(),
            "dir/subdir/file.txt"
        );
        assert_eq!(normalize_path("dir/subdir/").as_str(), "dir/subdir/");

        // Leading slash removal
        assert_eq!(normalize_path("/file.txt").as_str(), "file.txt");
        assert_eq!(normalize_path("/dir/").as_str(), "dir/");
        assert_eq!(
            normalize_path("/dir/subdir/file.txt").as_str(),
            "dir/subdir/file.txt"
        );

        // Dot segments
        assert_eq!(normalize_path("dir/./file.txt").as_str(), "dir/file.txt");
        assert_eq!(normalize_path("dir/./").as_str(), "dir/");

        // Double dot segments
        assert_eq!(
            normalize_path("dir/subdir/../file.txt").as_str(),
            "dir/file.txt"
        );
        assert_eq!(normalize_path("dir/subdir/../").as_str(), "dir/");

        // Multiple slashes
        assert_eq!(
            normalize_path("dir//subdir//file.txt").as_str(),
            "dir/subdir/file.txt"
        );
        assert_eq!(normalize_path("dir//subdir//").as_str(), "dir/subdir/");

        // Complex cases
        assert_eq!(
            normalize_path("/dir/./subdir/../another//file.txt").as_str(),
            "dir/another/file.txt"
        );
        assert_eq!(
            normalize_path("/dir/./subdir/../another//").as_str(),
            "dir/another/"
        );

        // Edge cases
        assert_eq!(normalize_path("").as_str(), "");
        assert_eq!(normalize_path("/").as_str(), "/");
        assert_eq!(normalize_path(".").as_str(), "");
        assert_eq!(normalize_path("./").as_str(), "/");
        assert_eq!(normalize_path("..").as_str(), "");
        assert_eq!(normalize_path("../").as_str(), "/");
    }

    #[test]
    fn test_normalize_path_glob_patterns() {
        // Basic glob patterns - should be preserved as-is
        assert_eq!(normalize_path("*.txt").as_str(), "*.txt");
        assert_eq!(normalize_path("*.rs").as_str(), "*.rs");
        assert_eq!(normalize_path("file.*").as_str(), "file.*");

        // Directory globs
        assert_eq!(normalize_path("*/").as_str(), "*/");
        assert_eq!(normalize_path("dir/*/").as_str(), "dir/*/");

        // Double asterisk (recursive glob)
        assert_eq!(normalize_path("**").as_str(), "**");
        assert_eq!(normalize_path("**/").as_str(), "**/");
        assert_eq!(normalize_path("**/*.txt").as_str(), "**/*.txt");
        assert_eq!(normalize_path("dir/**/*.rs").as_str(), "dir/**/*.rs");

        // Question mark patterns
        assert_eq!(normalize_path("file?.txt").as_str(), "file?.txt");
        assert_eq!(normalize_path("test?.*").as_str(), "test?.*");

        // Character classes
        assert_eq!(normalize_path("file[0-9].txt").as_str(), "file[0-9].txt");
        assert_eq!(normalize_path("[abc]*.rs").as_str(), "[abc]*.rs");
        assert_eq!(normalize_path("test[!0-9].txt").as_str(), "test[!0-9].txt");

        // Complex glob patterns
        assert_eq!(
            normalize_path("src/**/*.{rs,toml}").as_str(),
            "src/**/*.{rs,toml}"
        );
        assert_eq!(
            normalize_path("tests/**/test_*.rs").as_str(),
            "tests/**/test_*.rs"
        );

        // Globs with path normalization
        assert_eq!(normalize_path("dir/../*.txt").as_str(), "*.txt");
        assert_eq!(normalize_path("./src/**/*.rs").as_str(), "src/**/*.rs");
        assert_eq!(
            normalize_path("dir/./sub/**/*.txt").as_str(),
            "dir/sub/**/*.txt"
        );

        // Globs with leading slash removal
        assert_eq!(normalize_path("/*.txt").as_str(), "*.txt");
        assert_eq!(normalize_path("/**/").as_str(), "**/");
        assert_eq!(normalize_path("/dir/**/*.rs").as_str(), "dir/**/*.rs");

        // Globs with multiple slashes
        assert_eq!(normalize_path("dir//**/*.txt").as_str(), "dir/**/*.txt");
        assert_eq!(normalize_path("src//sub//*.rs").as_str(), "src/sub/*.rs");

        // Complex cases combining normalization and globs
        assert_eq!(
            normalize_path("/dir/./sub/../**/*.{rs,txt}").as_str(),
            "dir/**/*.{rs,txt}"
        );
        assert_eq!(
            normalize_path("./tests//unit/../integration/**/test_*.rs").as_str(),
            "tests/integration/**/test_*.rs"
        );

        // Edge cases with globs
        assert_eq!(normalize_path("*").as_str(), "*");
        assert_eq!(normalize_path("?").as_str(), "?");
        assert_eq!(normalize_path("[abc]").as_str(), "[abc]");
        assert_eq!(normalize_path("{}").as_str(), "{}");
    }

    #[tokio::test]
    async fn test_copy_glob_single_pattern() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create test files
        source.write("dir/file1.txt", "content1").await?;
        source.write("dir/file2.txt", "content2").await?;
        source.write("dir/file3.rs", "content3").await?;

        // Copy only .txt files
        copy(
            (source.clone(), "dir/*.txt".to_string()),
            (destination.clone(), "output/".to_string()),
        )
        .await?;

        // Verify .txt files were copied
        let buffer1 = destination.read("output/file1.txt").await?;
        assert_eq!(buffer1.to_vec(), b"content1");

        let buffer2 = destination.read("output/file2.txt").await?;
        assert_eq!(buffer2.to_vec(), b"content2");

        // Verify .rs file was NOT copied
        let result = destination.read("output/file3.rs").await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_glob_recursive_pattern() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create nested test files
        source.write("src/main.rs", "main").await?;
        source.write("src/lib.rs", "lib").await?;
        source.write("src/utils/helper.rs", "helper").await?;
        source.write("src/utils/mod.rs", "mod").await?;
        source.write("src/readme.txt", "readme").await?;

        // Copy all .rs files recursively
        copy(
            (source.clone(), "src/**/*.rs".to_string()),
            (destination.clone(), "backup/".to_string()),
        )
        .await?;

        // Verify .rs files were copied with directory structure preserved
        let main = destination.read("backup/main.rs").await?;
        assert_eq!(main.to_vec(), b"main");

        let lib = destination.read("backup/lib.rs").await?;
        assert_eq!(lib.to_vec(), b"lib");

        let helper = destination.read("backup/utils/helper.rs").await?;
        assert_eq!(helper.to_vec(), b"helper");

        let mod_rs = destination.read("backup/utils/mod.rs").await?;
        assert_eq!(mod_rs.to_vec(), b"mod");

        // Verify .txt file was NOT copied
        let result = destination.read("backup/readme.txt").await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_glob_question_mark_pattern() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create test files
        source.write("data/file1.txt", "one").await?;
        source.write("data/file2.txt", "two").await?;
        source.write("data/file10.txt", "ten").await?;

        // Copy files matching file?.txt (single character)
        copy(
            (source.clone(), "data/file?.txt".to_string()),
            (destination.clone(), "out/".to_string()),
        )
        .await?;

        // Verify single digit files were copied
        let f1 = destination.read("out/file1.txt").await?;
        assert_eq!(f1.to_vec(), b"one");

        let f2 = destination.read("out/file2.txt").await?;
        assert_eq!(f2.to_vec(), b"two");

        // Verify file10.txt was NOT copied (? matches only single character)
        let result = destination.read("out/file10.txt").await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_glob_character_class_pattern() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create test files
        source.write("logs/app.log", "app").await?;
        source.write("logs/app.txt", "txt").await?;
        source.write("logs/app.bak", "bak").await?;

        // Copy files with .log or .txt extension using character class
        copy(
            (source.clone(), "logs/app.[lt]*".to_string()),
            (destination.clone(), "archive/".to_string()),
        )
        .await?;

        // Verify matching files were copied
        let log = destination.read("archive/app.log").await?;
        assert_eq!(log.to_vec(), b"app");

        let txt = destination.read("archive/app.txt").await?;
        assert_eq!(txt.to_vec(), b"txt");

        // Verify .bak was NOT copied
        let result = destination.read("archive/app.bak").await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_glob_to_existing_directory() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source files
        source.write("src/a.txt", "a").await?;
        source.write("src/b.txt", "b").await?;

        // Pre-create destination directory with existing file
        destination.write("dest/existing.txt", "existing").await?;

        // Copy glob to existing directory
        copy(
            (source.clone(), "src/*.txt".to_string()),
            (destination.clone(), "dest/".to_string()),
        )
        .await?;

        // Verify files were copied
        let a = destination.read("dest/a.txt").await?;
        assert_eq!(a.to_vec(), b"a");

        let b = destination.read("dest/b.txt").await?;
        assert_eq!(b.to_vec(), b"b");

        // Verify existing file is still there
        let existing = destination.read("dest/existing.txt").await?;
        assert_eq!(existing.to_vec(), b"existing");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_glob_no_matches() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create test files
        source.write("dir/file.txt", "content").await?;

        // Try to copy with pattern that matches nothing
        copy(
            (source.clone(), "dir/*.rs".to_string()),
            (destination.clone(), "output/".to_string()),
        )
        .await?;

        // Destination directory should exist but contain no files
        let entries = destination.list("output/").await?;
        let file_entries: Vec<_> = entries.iter().filter(|e| e.metadata().is_file()).collect();
        assert!(file_entries.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_glob_to_file_should_error() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create source files
        source.write("src/a.txt", "a").await?;
        source.write("src/b.txt", "b").await?;

        // Create a file at destination
        destination.write("dest", "file").await?;

        // Try to copy glob to a file - should error
        let result = copy(
            (source.clone(), "src/*.txt".to_string()),
            (destination.clone(), "dest".to_string()),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotADirectory);

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_glob_preserves_nested_structure() -> Result<(), Error> {
        let source = Operator::new(Memory::default())?.finish();
        let destination = Operator::new(Memory::default())?.finish();

        // Create deeply nested structure
        source.write("project/src/main/app.rs", "app").await?;
        source
            .write("project/src/main/utils/helper.rs", "helper")
            .await?;
        source
            .write("project/src/test/test_app.rs", "test_app")
            .await?;
        source.write("project/docs/readme.md", "readme").await?;

        // Copy all .rs files
        copy(
            (source.clone(), "project/**/*.rs".to_string()),
            (destination.clone(), "backup/".to_string()),
        )
        .await?;

        // Verify structure is preserved
        let app = destination.read("backup/src/main/app.rs").await?;
        assert_eq!(app.to_vec(), b"app");

        let helper = destination.read("backup/src/main/utils/helper.rs").await?;
        assert_eq!(helper.to_vec(), b"helper");

        let test_app = destination.read("backup/src/test/test_app.rs").await?;
        assert_eq!(test_app.to_vec(), b"test_app");

        // Verify .md was NOT copied
        let result = destination.read("backup/docs/readme.md").await;
        assert!(result.is_err());

        Ok(())
    }
}
