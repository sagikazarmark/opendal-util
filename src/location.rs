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
