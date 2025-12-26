use std::collections::HashMap;

use opendal::{Error, ErrorKind, Operator, OperatorRegistry, OperatorUri};
use url::Url;

pub trait OperatorFactory: Send + Sync {
    fn from_uri(&self, uri: &str) -> Result<Operator, Error>;
}

pub struct DefaultOperatorFactory;

impl OperatorFactory for DefaultOperatorFactory {
    fn from_uri(&self, uri: &str) -> Result<Operator, Error> {
        Operator::from_uri(uri)
    }
}

pub struct RegistryOperatorFactory {
    registry: OperatorRegistry,
}

impl RegistryOperatorFactory {
    pub fn new(registry: OperatorRegistry) -> Self {
        Self { registry }
    }
}

impl OperatorFactory for RegistryOperatorFactory {
    fn from_uri(&self, uri: &str) -> Result<Operator, Error> {
        self.registry.load(uri)
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
    fn from_uri(&self, uri: &str) -> Result<Operator, Error> {
        let mut url = Url::parse(uri).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "Failed to parse uri").set_source(err)
        })?;

        let profile_name = url.scheme();

        let profile = self
            .profiles
            .get(profile_name)
            .ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "Profile not found")
                    .with_context("profile_name", profile_name)
            })?
            .clone();

        let scheme = profile.get("type").cloned().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "Missing 'type' in profile")
                .with_context("profile_name", profile_name)
        })?;

        let _ = url.set_scheme(scheme.as_str());

        let uri = OperatorUri::new(url.as_str(), profile)?;

        Operator::from_uri(uri)
    }
}

pub struct LambdaOperatorFactory<F> {
    inner: Box<dyn OperatorFactory>,
    r#fn: F,
}

impl<F> LambdaOperatorFactory<F>
where
    F: Fn(Operator) -> Operator + Send + Sync,
{
    pub fn new(inner: impl OperatorFactory + 'static, r#fn: F) -> Self {
        Self {
            inner: Box::new(inner),
            r#fn,
        }
    }
}

impl<F> OperatorFactory for LambdaOperatorFactory<F>
where
    F: Fn(Operator) -> Operator + Send + Sync,
{
    fn from_uri(&self, uri: &str) -> Result<Operator, Error> {
        let op = self.inner.from_uri(uri)?;

        Ok((self.r#fn)(op))
    }
}
