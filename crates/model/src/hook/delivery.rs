use std::fmt;

/// A delivery setting such as a URL or a secret.
#[derive(Clone, PartialEq, Eq)]
pub enum Setting {
    Value(String),
    /// Why there is no value.
    Unset(String),
}

impl Setting {
    pub fn get(&self) -> Result<&str, &str> {
        match self {
            Setting::Value(v) => Ok(v),
            Setting::Unset(why) => Err(why),
        }
    }
}

impl fmt::Debug for Setting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Setting::Value(_) => f.write_str("Value(..)"),
            Setting::Unset(why) => f.debug_tuple("Unset").field(why).finish(),
        }
    }
}

/// Where a hook posts.
#[derive(Clone, PartialEq, Eq)]
pub enum Delivery {
    Slack { url: Setting },
    Webhook { url: Setting, secret: Setting },
}

impl Delivery {
    pub fn kind(&self) -> &'static str {
        match self {
            Delivery::Slack { .. } => "slack",
            Delivery::Webhook { .. } => "webhook",
        }
    }

    pub fn url(&self) -> &Setting {
        match self {
            Delivery::Slack { url } | Delivery::Webhook { url, .. } => url,
        }
    }

    pub fn unset(&self) -> Option<&str> {
        match self {
            Delivery::Slack { url } => url.get().err(),
            Delivery::Webhook { url, secret } => url.get().err().or_else(|| secret.get().err()),
        }
    }

    pub fn host(&self) -> Option<&str> {
        self.url().get().ok().and_then(url_host)
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.kind(), self.host().unwrap_or("?"))
    }
}

/// The host of an absolute URL, without port or userinfo.
pub fn url_host(url: &str) -> Option<&str> {
    let after_scheme = url.split_once("://")?.1;
    let authority = after_scheme.split(['/', '?', '#']).next()?;
    let host_port = authority.rsplit('@').next()?;
    let host = match host_port.strip_prefix('[') {
        Some(v6) => v6.split(']').next()?,
        None => host_port.split(':').next()?,
    };

    (!host.is_empty()).then_some(host)
}
