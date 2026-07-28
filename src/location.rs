use percent_encoding::percent_decode_str;
use url::Url;

mod sealed {
    use url::Url;

    pub trait Sealed {
        fn example() -> Self;
    }

    impl Sealed for Url {
        fn example() -> Self {
            Url::parse("https://example.com/path/to/file.pdf").unwrap()
        }
    }
    impl Sealed for String {
        fn example() -> Self {
            "path/to/file.pdf".to_string()
        }
    }
}

#[cfg(feature = "serde")]
pub trait SerdeSupport: serde::Serialize + serde::de::DeserializeOwned {}

#[cfg(not(feature = "serde"))]
pub trait SerdeSupport {}

// Helper trait for schemars support
#[cfg(feature = "schemars")]
pub trait SchemaSupport: schemars::JsonSchema {}

#[cfg(not(feature = "schemars"))]
pub trait SchemaSupport {}

#[cfg(feature = "serde")]
impl<T> SerdeSupport for T where T: serde::Serialize + serde::de::DeserializeOwned {}

#[cfg(not(feature = "serde"))]
impl<T> SerdeSupport for T {}

#[cfg(feature = "schemars")]
impl<T> SchemaSupport for T where T: schemars::JsonSchema {}

#[cfg(not(feature = "schemars"))]
impl<T> SchemaSupport for T {}

pub trait LocationType: sealed::Sealed + SerdeSupport + SchemaSupport {}

impl LocationType for Url {}
impl LocationType for String {}

/// Split a location URI into the backend base URI and the object path.
///
/// The authority names the container (bucket, container, endpoint host — whichever the
/// backend maps its authority to) and the path names the object within it. Splitting
/// before handing the base to a factory is what keeps [`opendal::Operator::from_uri`]
/// from folding the object path into the operator's `root`.
///
/// The query string stays with the base, since it carries backend options. The fragment
/// is dropped: it belongs to neither half of the split. Note this does not rescue a key
/// containing an unencoded `#` — per RFC 3986 that starts a fragment, so `Url` has already
/// cut the path short before this function sees it. A `#` in a key must arrive
/// percent-encoded as `%23`.
///
/// The returned path is percent-decoded — lossily, exactly as OpenDAL's own `OperatorUri`
/// does, so invalid UTF-8 sequences become U+FFFD rather than an error. Two locations
/// differing only in invalid sequences therefore collapse onto the same key. Leading `/`
/// characters are then removed, all of them, after decoding as `OperatorUri` also does.
/// A trailing `/` is preserved, since it distinguishes a directory from a file; that
/// follows `opendal::raw::normalize_path` rather than `OperatorUri`, which trims the
/// trailing slash off the `root` it derives.
///
/// An omitted authority is always spelled `scheme://`. `Url` renders such a URI as
/// `scheme:`, and OpenDAL reads a base without `://` as a scheme *verbatim* — so the bare
/// form would yield a scheme named `"s3:"` that no registry knows.
///
/// ```
/// # use url::Url;
/// # use opendal_util::split_location;
/// let (base, path) = split_location(&Url::parse("s3://bucket/a/b.pdf").unwrap());
/// assert_eq!(base, "s3://bucket");
/// assert_eq!(path, "a/b.pdf");
/// ```
pub fn split_location(location: &Url) -> (String, String) {
    let path = percent_decode_str(location.path())
        .decode_utf8_lossy()
        .trim_start_matches('/')
        .to_string();

    let mut base = location.clone();
    base.set_path("");
    base.set_fragment(None);

    let mut base = base.to_string();

    // `s3:/a/b.pdf` and `s3:a/b.pdf` name no authority, and `Url` renders both bases as
    // `s3:`. Spell the empty authority out so OperatorUri parses the base as a URI
    // instead of taking `s3:` for a scheme name. A scheme cannot contain `:`, so the
    // first one always separates it from the rest.
    //
    // The test is `has_authority`, not `base.contains("://")` — the latter is also
    // satisfied by a query value like `?endpoint=http://minio:9000`, which would leave
    // exactly the bases this is meant to fix unnormalized.
    if !location.has_authority() {
        base = base.replacen(':', "://", 1);
    }

    (base, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(uri: &str) -> (String, String) {
        split_location(&Url::parse(uri).expect("valid uri"))
    }

    #[test]
    fn splits_authority_from_path() {
        assert_eq!(
            split("s3://bucket/a/b.pdf"),
            ("s3://bucket".into(), "a/b.pdf".into())
        );
    }

    /// The whole point of splitting: `Operator::from_uri` folds a URI path component into
    /// `root`, so the base handed to the factory must carry no path.
    #[test]
    fn base_never_carries_the_object_path() {
        let (base, _) = split("s3://bucket/deeply/nested/key.pdf");
        assert_eq!(base, "s3://bucket");
    }

    /// A bare container yields an empty path. Handlers that need a key must reject this
    /// rather than operate on the container root.
    #[test]
    fn bare_container_yields_empty_path() {
        assert_eq!(split("s3://bucket"), ("s3://bucket".into(), "".into()));
        assert_eq!(split("s3://bucket/"), ("s3://bucket".into(), "".into()));
    }

    /// `s3://report.pdf` reads the filename as the container — the authority slot is not
    /// optional in this grammar. The empty path is what surfaces the mistake.
    #[test]
    fn filename_without_container_is_read_as_a_container() {
        assert_eq!(
            split("s3://report.pdf"),
            ("s3://report.pdf".into(), "".into())
        );
    }

    /// Omitting the authority is spelled with three slashes; the profile then supplies
    /// the container.
    #[test]
    fn omitted_authority_leaves_base_bare() {
        assert_eq!(
            split("prof:///a/b.pdf"),
            ("prof://".into(), "a/b.pdf".into())
        );
    }

    /// One- and two-slash forms name no authority either, and `Url` renders both bases as
    /// `prof:`. OperatorUri reads a base without `://` as a scheme verbatim, so leaving
    /// the colon on yields a scheme named `"prof:"` — the same failure the factory's
    /// `Url::set_scheme` bug produced. All three spellings must agree.
    #[test]
    fn authority_less_forms_all_spell_the_empty_authority() {
        assert_eq!(split("prof:/a/b.pdf"), ("prof://".into(), "a/b.pdf".into()));
        assert_eq!(split("prof:a/b.pdf"), ("prof://".into(), "a/b.pdf".into()));
        assert_eq!(split("prof:"), ("prof://".into(), "".into()));
    }

    /// A query on an authority-less location must stay a query. Emitting the bare scheme
    /// `prof` instead would fold it into the scheme name as `prof?region=us-east-1`.
    ///
    /// The second case is why the empty authority is detected with `has_authority` rather
    /// than by looking for `://` in the serialized base: an endpoint option supplies a
    /// `://` of its own, which would leave exactly this form unnormalized.
    #[test]
    fn authority_less_base_keeps_its_query() {
        let (base, path) = split("prof:a/b.pdf?region=us-east-1");
        assert_eq!(base, "prof://?region=us-east-1");
        assert_eq!(path, "a/b.pdf");

        let (base, path) = split("prof:a/b.pdf?endpoint=http://minio:9000");
        assert_eq!(base, "prof://?endpoint=http://minio:9000");
        assert_eq!(path, "a/b.pdf");
    }

    /// The fragment is not a backend option, and `#` is a legal object key byte — leaving
    /// it on the base would silently truncate the key at the `#`.
    #[test]
    fn fragment_is_dropped_from_the_base() {
        assert_eq!(
            split("s3://bucket/a/b.pdf#page=3"),
            ("s3://bucket".into(), "a/b.pdf".into())
        );
        assert_eq!(
            split("s3://bucket/report%233.pdf"),
            ("s3://bucket".into(), "report#3.pdf".into())
        );
    }

    #[test]
    fn percent_encoded_path_is_decoded() {
        assert_eq!(
            split("s3://bucket/Q1%20Summary/final%20v2.pdf"),
            ("s3://bucket".into(), "Q1 Summary/final v2.pdf".into())
        );
        assert_eq!(
            split("s3://bucket/%C3%BCnicode.pdf"),
            ("s3://bucket".into(), "ünicode.pdf".into())
        );
    }

    #[test]
    fn path_case_is_preserved() {
        assert_eq!(
            split("s3://MyBucket/MyFolder/Report.PDF"),
            ("s3://MyBucket".into(), "MyFolder/Report.PDF".into())
        );
    }

    /// Trailing slashes distinguish directories from files in OpenDAL.
    #[test]
    fn trailing_slash_is_preserved() {
        assert_eq!(
            split("s3://bucket/prefix/"),
            ("s3://bucket".into(), "prefix/".into())
        );
    }

    /// Query parameters carry backend options, so they belong with the base.
    #[test]
    fn query_stays_with_the_base() {
        let (base, path) = split("s3://bucket/a/b.pdf?region=us-east-1");
        assert_eq!(base, "s3://bucket?region=us-east-1");
        assert_eq!(path, "a/b.pdf");
    }
}
