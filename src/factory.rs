use std::collections::HashMap;

use opendal::{Error, ErrorKind, Operator, OperatorRegistry, OperatorUri};
use url::Url;

pub trait OperatorFactory: Send + Sync {
    /// Build an operator from a backend URI.
    ///
    /// A path component feeds the operator's `root`, as it does for
    /// [`Operator::from_uri`] — though whether it wins over a `root` set in the backend's
    /// own config is per-service: s3 and http let the URI override, memory and fs keep
    /// the configured value. Callers addressing an object within the backend want
    /// [`resolve`](Self::resolve) instead, which leaves `root` alone entirely.
    fn load(&self, uri: &str) -> Result<Operator, Error>;

    /// Resolve a location URI into the operator that serves it and the object path within.
    ///
    /// Splitting before loading is what keeps [`Operator::from_uri`] from folding the
    /// object path into the operator's `root`. See [`crate::split_location`].
    ///
    /// The returned path is empty when the location names only a container; callers that
    /// require a key must reject that rather than operate on the container root.
    fn resolve(&self, location: &Url) -> Result<(Operator, String), Error> {
        let (base, path) = crate::split_location(location);

        Ok((self.load(&base)?, path))
    }
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
        // `Unsupported`, not `ConfigInvalid`, so ChainOperatorFactory falls through: a URI
        // this factory cannot parse may still be one the next factory handles. The bare
        // scheme form `memory` is exactly that — Url rejects it as a relative URL, while
        // Operator::from_uri accepts it.
        let url = Url::parse(uri).map_err(|err| {
            Error::new(ErrorKind::Unsupported, "Failed to parse uri").set_source(err)
        })?;

        let profile_name = url.scheme();

        let entries = self.profiles.get(profile_name).ok_or_else(|| {
            // Operator::from_uri returns this error as well when a scheme is unsupported,
            // even though the error description says this error is returned when an operation is not supported.
            Error::new(ErrorKind::Unsupported, "Profile not found")
                .with_context("profile_name", profile_name)
        })?;

        // Keys are folded to lowercase so `type` is found however it was spelled and so a
        // profile entry reliably displaces a query parameter differing only in case.
        // Collisions are rejected rather than resolved: HashMap iteration order is
        // randomized, so letting the last writer win would pick between `Type` and `TYPE`
        // differently from run to run.
        let mut profile: HashMap<String, &String> = HashMap::with_capacity(entries.len());
        for (key, value) in entries {
            if profile.insert(key.to_ascii_lowercase(), value).is_some() {
                return Err(
                    Error::new(ErrorKind::ConfigInvalid, "Duplicate key in profile")
                        .with_context("profile_name", profile_name)
                        .with_context("key", key),
                );
            }
        }

        let scheme = profile.get("type").ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "Missing 'type' in profile")
                .with_context("profile_name", profile_name)
        })?;

        // The scheme is interpolated into the base below, so a value like `s3://elsewhere`
        // would otherwise smuggle in its own authority and silently redirect the operator
        // at a different backend. Only RFC 3986 scheme syntax gets through.
        if scheme.is_empty()
            || !scheme.starts_with(|c: char| c.is_ascii_alphabetic())
            || !scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "Invalid 'type' in profile")
                    .with_context("profile_name", profile_name)
                    .with_context("type", *scheme),
            );
        }

        // Query parameters first, profile second, so the profile wins for any key it
        // sets. This mirrors OperatorUri's own precedence, but we apply it here because
        // the base is rebuilt below and OperatorUri would otherwise never see the query.
        //
        // Note this settles only the query-vs-profile question. Service `from_uri` impls
        // re-apply the keys they derive from the URI itself afterwards — s3 overwrites
        // `bucket` from the authority, http overwrites `endpoint`, and both s3 and http
        // overwrite `root` from the path — so for those keys the location still beats the
        // profile. Which side wins for `root` is per-service: memory and fs keep the
        // profile's value instead.
        let mut options = url
            .query_pairs()
            .map(|(key, value)| (key.to_ascii_lowercase(), value.into_owned()))
            .collect::<HashMap<_, _>>();

        options.extend(
            profile
                .iter()
                // `type` selects the profile's backend; it is not a backend config key.
                .filter(|(key, _)| key.as_str() != "type")
                .map(|(key, value)| (key.clone(), (*value).clone())),
        );

        // Rebuild the base with the backend scheme rather than rewriting the scheme on
        // the parsed URL. `Url::set_scheme` turns `store:` into `memory:`, and
        // OperatorUri reads a base with no `://` as a scheme verbatim — yielding a scheme
        // named "memory:" that the registry does not know.
        //
        // The path is carried over so this behaves like every other OperatorFactory:
        // it becomes the operator's `root`. `resolve` is what strips the object path.
        let authority = url.authority();
        let path = url.path();
        // Url omits the leading `/` for an authority-less URI like `store:a/b.pdf`;
        // without it the first path segment would be re-read as the authority.
        let separator = if path.is_empty() || path.starts_with('/') {
            ""
        } else {
            "/"
        };
        let base = format!("{scheme}://{authority}{separator}{path}");

        Operator::from_uri(OperatorUri::new(&base, options)?)
    }
}

#[cfg(test)]
mod profile_tests {
    use super::*;

    fn factory() -> ProfileOperatorFactory {
        let profile = HashMap::from([("type".to_string(), "memory".to_string())]);

        ProfileOperatorFactory::new(HashMap::from([("store".to_string(), profile)]))
    }

    /// Only the bare `store:` form is the regression test for the `Url::set_scheme` bug:
    /// that path produced `memory:`, which `OperatorUri` read as a scheme *named*
    /// `"memory:"`, and the registry rejected it. The rest are controls — all already
    /// contained `://` after the scheme swap, so they resolved before the fix too.
    #[test]
    fn resolves_every_authority_form() {
        factory().load("store:").expect("bare `store:` resolves");
        factory()
            .load("store://")
            .expect("empty authority resolves");
        factory()
            .load("store://container")
            .expect("authority resolves");
        factory().load("store:/a/b").expect("one slash resolves");
        factory()
            .load("store:///a/b")
            .expect("three slashes resolve");
    }

    /// Both spellings of a colliding key are equally valid inputs, and `HashMap` iteration
    /// order is randomized — so picking one silently would make the resulting backend
    /// config differ from run to run. Rejecting is the only deterministic answer.
    #[test]
    fn colliding_profile_keys_are_rejected() {
        let profile = HashMap::from([
            ("type".to_string(), "memory".to_string()),
            ("TYPE".to_string(), "fs".to_string()),
        ]);
        let factory = ProfileOperatorFactory::new(HashMap::from([("store".to_string(), profile)]));

        for _ in 0..64 {
            assert_eq!(
                factory.load("store://c").unwrap_err().kind(),
                ErrorKind::ConfigInvalid
            );
        }
    }

    /// `type` is interpolated into the rebuilt base, so a value carrying its own authority
    /// would silently point the operator at a different backend — `s3://elsewhere` demotes
    /// the location's container into the root and writes to `elsewhere` instead.
    #[test]
    fn profile_type_carrying_an_authority_is_rejected() {
        for bad in ["memory://elsewhere", "memory:", "", "1memory", "mem ory"] {
            let profile = HashMap::from([("type".to_string(), bad.to_string())]);
            let factory =
                ProfileOperatorFactory::new(HashMap::from([("store".to_string(), profile)]));

            assert_eq!(
                factory.load("store://container/prefix").unwrap_err().kind(),
                ErrorKind::ConfigInvalid,
                "`type = {bad:?}` must be rejected"
            );
        }
    }

    /// `load` is the base-URI entry point and treats a path as `root`, exactly as
    /// `DefaultOperatorFactory` does. Rebuilding the base from scheme and authority alone
    /// would silently drop it and leave the operator at the container root.
    #[test]
    fn load_keeps_a_path_as_the_operator_root() {
        let operator = factory()
            .load("store://container/prefix/inner")
            .expect("profile resolves");

        assert_eq!(operator.info().root(), "/prefix/inner/");
    }

    /// `resolve` is what strips the object path, so the operator it returns is rooted at
    /// the container and the key comes back separately.
    #[tokio::test]
    async fn resolve_splits_the_object_path_off_the_operator() {
        let location = Url::parse("store://container/prefix/inner.pdf").unwrap();
        let (operator, path) = factory().resolve(&location).expect("profile resolves");

        assert_eq!(operator.info().root(), "/");
        assert_eq!(path, "prefix/inner.pdf");

        operator.write(&path, "data").await.expect("write succeeds");
    }

    /// `ChainOperatorFactory` only falls through on `Unsupported`, so an unknown profile
    /// must report that and not `ConfigInvalid`.
    #[test]
    fn unknown_profile_reports_unsupported() {
        let err = factory().load("other://x").unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Unsupported);
    }

    /// A URI this factory cannot even parse must also fall through — `Operator::from_uri`
    /// accepts the bare scheme `memory`, which `Url::parse` rejects as a relative URL.
    #[test]
    fn unparseable_uri_falls_through_the_chain() {
        assert_eq!(
            factory().load("memory").unwrap_err().kind(),
            ErrorKind::Unsupported
        );

        let chain = ChainOperatorFactory::builder()
            .then(factory())
            .then(DefaultOperatorFactory)
            .build();

        chain.load("memory").expect("default factory handles it");
    }

    /// End-to-end check that the split hands OpenDAL a *decoded* key. Passing the raw
    /// URI path through would store the object under the literal key `my%20file.pdf`,
    /// since OpenDAL's `normalize_path` trims slashes but never percent-decodes.
    #[tokio::test]
    async fn percent_encoded_key_round_trips_decoded() {
        let location = Url::parse("store://container/reports/my%20file.pdf").unwrap();
        let (operator, path) = factory().resolve(&location).expect("profile resolves");

        operator.write(&path, "data").await.expect("write succeeds");

        let stored = operator
            .read("reports/my file.pdf")
            .await
            .expect("object is stored under the decoded key");

        assert_eq!(stored.to_vec(), b"data");
    }

    #[test]
    fn profile_without_type_is_config_invalid() {
        let factory =
            ProfileOperatorFactory::new(HashMap::from([("broken".to_string(), HashMap::new())]));

        assert_eq!(
            factory.load("broken://x").unwrap_err().kind(),
            ErrorKind::ConfigInvalid
        );
    }

    /// Every other profile key is matched case-insensitively, so `type` must be too —
    /// otherwise a profile written with `Type` reports a missing `type`.
    #[test]
    fn profile_type_lookup_is_case_insensitive() {
        let profile = HashMap::from([("Type".to_string(), "memory".to_string())]);
        let factory = ProfileOperatorFactory::new(HashMap::from([("store".to_string(), profile)]));

        factory.load("store://container").expect("profile resolves");
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

    /// Delegates to each inner factory's `resolve` rather than inheriting the default,
    /// which would call `self.load` and so bypass any override an inner factory made.
    fn resolve(&self, location: &Url) -> Result<(Operator, String), Error> {
        for factory in &self.factories {
            match factory.resolve(location) {
                Ok(resolved) => return Ok(resolved),
                Err(e) if e.kind() == ErrorKind::Unsupported => continue,
                Err(e) => return Err(e),
            }
        }

        Err(Error::new(ErrorKind::Unsupported, "Unsupported URI")
            .with_context("uri", location.as_str()))
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

    /// Delegates to the inner factory's `resolve` so an override there is not bypassed,
    /// then applies the transform to the operator it produced.
    fn resolve(&self, location: &Url) -> Result<(Operator, String), Error> {
        let (op, path) = self.inner.resolve(location)?;

        Ok(((self.transform)(op), path))
    }
}

#[cfg(test)]
mod wrapper_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// Overrides `resolve` with a recognisable path. A wrapper that inherits the default
    /// `resolve` routes through `load` instead and never reaches this, so the sentinel is
    /// what makes the delegation observable.
    struct SentinelFactory;

    impl OperatorFactory for SentinelFactory {
        fn load(&self, uri: &str) -> Result<Operator, Error> {
            DefaultOperatorFactory.load(uri)
        }

        fn resolve(&self, location: &Url) -> Result<(Operator, String), Error> {
            let (operator, _) = DefaultOperatorFactory.resolve(location)?;

            Ok((operator, "sentinel".to_string()))
        }
    }

    fn location() -> Url {
        Url::parse("memory://container/prefix/inner.pdf").unwrap()
    }

    #[test]
    fn chain_delegates_to_inner_resolve() {
        let chain = ChainOperatorFactory::builder()
            .then(SentinelFactory)
            .build();

        let (_, path) = chain.resolve(&location()).expect("resolves");

        assert_eq!(path, "sentinel");
    }

    #[test]
    fn lambda_delegates_to_inner_resolve() {
        let lambda = LambdaOperatorFactory::new(SentinelFactory, |op| op);

        let (_, path) = lambda.resolve(&location()).expect("resolves");

        assert_eq!(path, "sentinel");
    }

    /// Delegating must not cost the transform — it still has to reach the operator that
    /// `resolve` hands back.
    #[test]
    fn lambda_applies_its_transform_through_resolve() {
        let applied = Arc::new(AtomicBool::new(false));
        let flag = applied.clone();

        let lambda = LambdaOperatorFactory::new(SentinelFactory, move |op| {
            flag.store(true, Ordering::SeqCst);
            op
        });

        lambda.resolve(&location()).expect("resolves");

        assert!(applied.load(Ordering::SeqCst), "transform must run");
    }
}
