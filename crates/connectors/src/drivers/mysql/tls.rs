use crate::error::DriverError;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use std::path::PathBuf;

/// Build a connection pool from a URL.
pub(crate) fn pool_from_url(url: &str) -> Result<Pool, DriverError> {
    Ok(Pool::new(opts_from_url(url)?))
}

/// A pool of exactly one connection that runs `setup` on connect and again
/// after every reset.
pub(crate) fn pinned_pool_from_url(url: &str, setup: Vec<String>) -> Result<Pool, DriverError> {
    let one = PoolConstraints::new(1, 1)
        .ok_or_else(|| DriverError::ConnectionError("invalid pool constraints".into()))?;
    let opts = OptsBuilder::from_opts(opts_from_url(url)?)
        .setup(setup)
        .pool_opts(PoolOpts::default().with_constraints(one));
    Ok(Pool::new(opts))
}

/// Connection options from a URL, with an `ssl_ca` parameter turned into a
/// root certificate (mysql_async rejects parameters it doesn't know).
fn opts_from_url(url: &str) -> Result<Opts, DriverError> {
    let (cleaned, ca_path) = extract_ssl_ca(url)?;

    let opts = Opts::from_url(&cleaned).map_err(|e| DriverError::ConnectionError(e.to_string()))?;

    let opts = match ca_path {
        Some(path) => {
            let ssl = opts
                .ssl_opts()
                .cloned()
                .unwrap_or_default()
                .with_root_certs(vec![PathBuf::from(path).into()]);
            Opts::from(OptsBuilder::from_opts(opts).ssl_opts(ssl))
        }
        None => opts,
    };

    Ok(opts)
}

/// Split an `ssl_ca` parameter out of the URL (mysql_async rejects unknown
/// parameters). Returns the URL without `ssl_ca` and the extracted path, if
/// any. All other parameters are preserved untouched.
fn extract_ssl_ca(url: &str) -> Result<(String, Option<String>), DriverError> {
    let mut parsed = url::Url::parse(url).map_err(|e| DriverError::InvalidUrl(e.to_string()))?;

    let mut ca_path: Option<String> = None;
    let mut has_require_ssl = false;
    let mut kept: Vec<(String, String)> = parsed
        .query_pairs()
        .filter_map(|(k, v)| {
            if k == "ssl_ca" {
                ca_path = Some(v.into_owned());
                None
            } else {
                if k == "require_ssl" {
                    has_require_ssl = true;
                }
                Some((k.into_owned(), v.into_owned()))
            }
        })
        .collect();

    // Only rewrite the query when `ssl_ca` was present, to avoid perturbing
    // URLs that don't use it.
    if ca_path.is_some() {
        if !has_require_ssl {
            kept.push(("require_ssl".to_string(), "true".to_string()));
        }

        let mut pairs = parsed.query_pairs_mut();
        pairs.clear();
        for (k, v) in &kept {
            pairs.append_pair(k, v);
        }
    }

    Ok((parsed.to_string(), ca_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_of(url: &str) -> Option<String> {
        url::Url::parse(url).unwrap().query().map(str::to_owned)
    }

    #[test]
    fn extracts_and_strips_ssl_ca() {
        let (out, ca) =
            extract_ssl_ca("mysql://u:p@host:3306/db?require_ssl=true&ssl_ca=/tmp/ca.pem").unwrap();
        assert_eq!(ca.as_deref(), Some("/tmp/ca.pem"));
        let q = query_of(&out).unwrap();
        assert!(
            q.contains("require_ssl=true"),
            "other params preserved: {q}"
        );
        assert!(!q.contains("ssl_ca"), "ssl_ca stripped: {q}");
    }

    #[test]
    fn url_without_ssl_ca_is_unchanged() {
        let url = "mysql://u:p@host:3306/db?require_ssl=true";
        let (out, ca) = extract_ssl_ca(url).unwrap();
        assert_eq!(ca, None);
        assert_eq!(out, url);
    }

    #[test]
    fn ssl_ca_injects_require_ssl_when_absent() {
        let (out, ca) = extract_ssl_ca("mysql://u:p@host:3306/db?ssl_ca=/tmp/ca.pem").unwrap();
        assert_eq!(ca.as_deref(), Some("/tmp/ca.pem"));
        let q = query_of(&out).unwrap();
        assert!(q.contains("require_ssl=true"), "require_ssl injected: {q}");
        assert!(!q.contains("ssl_ca"), "ssl_ca stripped: {q}");
    }

    #[test]
    fn ssl_ca_does_not_duplicate_existing_require_ssl() {
        let (out, _) =
            extract_ssl_ca("mysql://u:p@host:3306/db?require_ssl=true&ssl_ca=/tmp/ca.pem").unwrap();
        let q = query_of(&out).unwrap();
        assert_eq!(
            q.matches("require_ssl").count(),
            1,
            "no duplicate require_ssl: {q}"
        );
    }
}
