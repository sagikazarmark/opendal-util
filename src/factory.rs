use std::collections::HashMap;

use opendal::{Error, ErrorKind, Operator, OperatorRegistry, OperatorUri};
use url::Url;

pub trait OperatorFactory: Send + Sync {
    fn load(&self, uri: &str) -> Result<Operator, Error>;
}

pub struct DefaultOperatorFactory;

impl DefaultOperatorFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultOperatorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl OperatorFactory for DefaultOperatorFactory {
    fn load(&self, uri: &str) -> Result<Operator, Error> {
        Operator::from_uri(uri)
    }
}

impl OperatorFactory for OperatorRegistry {
    fn load(&self, uri: &str) -> Result<Operator, Error> {
        OperatorRegistry::load(self, uri)
    }
}

pub struct ProfileOperatorFactory {
    profiles: HashMap<String, HashMap<String, String>>,
}

impl ProfileOperatorFactory {
    pub fn new(profiles: HashMap<String, HashMap<String, String>>) -> Self {
        Self { profiles }
    }
}

impl OperatorFactory for ProfileOperatorFactory {
    fn load(&self, uri: &str) -> Result<Operator, Error> {
        let mut url = Url::parse(uri).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "Failed to parse uri").set_source(err)
        })?;

        let profile_name = url.scheme();

        let profile = self
            .profiles
            .get(profile_name)
            .ok_or_else(|| {
                // Operator::from_uri returns this error as well when a scheme is unsupported,
                // even though the error description says this error is returned when an operation is not supported.
                Error::new(ErrorKind::Unsupported, "Profile not found")
                    .with_context("profile_name", profile_name)
            })?
            .clone();

        let scheme = profile.get("type").cloned().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "Missing 'type' in profile")
                .with_context("profile_name", profile_name)
        })?;

        // This should never fail (sagikazarmark, 2025)
        let _ = url.set_scheme(scheme.as_str());

        let uri = OperatorUri::new(url.as_str(), profile)?;

        Operator::from_uri(uri)
    }
}

pub struct ChainOperatorFactory {
    factories: Vec<Box<dyn OperatorFactory>>,
}

impl ChainOperatorFactory {
    pub fn new<I>(factories: I) -> Self
    where
        I: IntoIterator<Item = Box<dyn OperatorFactory>>,
    {
        Self {
            factories: factories.into_iter().collect(),
        }
    }

    pub fn builder() -> ChainOperatorFactoryBuilder {
        ChainOperatorFactoryBuilder::default()
    }
}

impl OperatorFactory for ChainOperatorFactory {
    fn load(&self, uri: &str) -> Result<Operator, Error> {
        for factory in &self.factories {
            match factory.load(uri) {
                Ok(op) => return Ok(op),
                Err(e) if e.kind() == ErrorKind::Unsupported => continue,
                Err(e) => return Err(e),
            }
        }

        Err(Error::new(ErrorKind::Unsupported, "Unsupported URI").with_context("uri", uri))
    }
}

#[derive(Default)]
pub struct ChainOperatorFactoryBuilder {
    factories: Vec<Box<dyn OperatorFactory>>,
}

impl ChainOperatorFactoryBuilder {
    pub fn then(mut self, factory: impl OperatorFactory + 'static) -> Self {
        self.factories.push(Box::new(factory));
        self
    }

    pub fn build(self) -> ChainOperatorFactory {
        ChainOperatorFactory {
            factories: self.factories,
        }
    }
}

pub struct LambdaOperatorFactory<Inner, F>
where
    Inner: OperatorFactory,
    F: Fn(Operator) -> Operator + Send + Sync,
{
    inner: Inner,
    transform: F,
}

impl<Inner, F> LambdaOperatorFactory<Inner, F>
where
    Inner: OperatorFactory,
    F: Fn(Operator) -> Operator + Send + Sync,
{
    pub fn new(inner: Inner, transform: F) -> Self {
        Self { inner, transform }
    }
}

impl<Inner, F> OperatorFactory for LambdaOperatorFactory<Inner, F>
where
    Inner: OperatorFactory,
    F: Fn(Operator) -> Operator + Send + Sync,
{
    fn load(&self, uri: &str) -> Result<Operator, Error> {
        let op = self.inner.load(uri)?;

        Ok((self.transform)(op))
    }
}
