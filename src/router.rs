use std::{io::Read, path::Path};

use geosite_rs::{GeoSiteList, decode_geosite, domain::Type as GeoDomainType};
use log::{info, warn};
use regex::Regex;
use ustr::ustr;

use crate::config::RouteConfig;

enum MatchRule {
    Exact(ustr::Ustr),
    Suffix(ustr::Ustr),
    WildcardSuffix(ustr::Ustr),
    Keyword(ustr::Ustr),
    Regex(Regex),
}

pub struct Router {
    rules: Vec<(MatchRule, String)>,
    default_group: String,
}

impl Router {
    pub fn new(
        routes: &[RouteConfig],
        default_group: &str,
        geosite_data: Option<&GeoSiteList>,
    ) -> Self {
        let mut rules = Vec::new();

        for route in routes {
            for domain in &route.domains {
                let lower = domain.to_ascii_lowercase();
                if let Some(suffix) = lower.strip_prefix("*.") {
                    rules.push((MatchRule::WildcardSuffix(ustr(suffix)), route.group.clone()));
                } else {
                    rules.push((MatchRule::Exact(ustr(&lower)), route.group.clone()));
                    rules.push((
                        MatchRule::Suffix(ustr(&format!(".{lower}"))),
                        route.group.clone(),
                    ));
                }
            }

            if let Some(geosite) = geosite_data {
                for tag in &route.geosite {
                    add_geosite_group(&mut rules, geosite, tag, &route.group);
                }
            }
        }

        info!(target: "pasdns::router", "router built with {} rules", rules.len());
        Self {
            rules,
            default_group: default_group.to_string(),
        }
    }

    pub fn route(&self, domain: &str) -> &str {
        let lower = domain.to_ascii_lowercase();
        for (rule, group) in &self.rules {
            match rule {
                MatchRule::Exact(e) if lower == e.as_str() => return group,
                MatchRule::Suffix(s) if lower.ends_with(s.as_str()) => return group,
                MatchRule::WildcardSuffix(s)
                    if lower == s.as_str() || lower.ends_with(&format!(".{s}")) =>
                {
                    return group;
                }
                MatchRule::Keyword(k) if lower.contains(k.as_str()) => return group,
                MatchRule::Regex(re) if re.is_match(&lower) => return group,
                _ => {}
            }
        }
        &self.default_group
    }
}

fn add_geosite_group(
    rules: &mut Vec<(MatchRule, String)>,
    geosite: &GeoSiteList,
    tag: &str,
    group: &str,
) {
    let Some(entry) = geosite.entry.iter().find(|e| e.country_code == tag) else {
        warn!(target: "pasdns::router", "geosite tag not found: {tag}");
        return;
    };

    let count = entry.domain.len();
    for domain in &entry.domain {
        let value = domain.value.to_ascii_lowercase();
        match GeoDomainType::try_from(domain.r#type).unwrap_or(GeoDomainType::Plain) {
            GeoDomainType::Plain => {
                rules.push((MatchRule::Keyword(ustr(&value)), group.to_string()));
            }
            GeoDomainType::Regex => match Regex::new(&value) {
                Ok(re) => rules.push((MatchRule::Regex(re), group.to_string())),
                Err(e) => {
                    warn!(target: "pasdns::router", "invalid geosite regex '{}': {e}", value)
                }
            },
            GeoDomainType::Domain => {
                rules.push((MatchRule::WildcardSuffix(ustr(&value)), group.to_string()));
                rules.push((
                    MatchRule::Suffix(ustr(&format!(".{value}"))),
                    group.to_string(),
                ));
            }
            GeoDomainType::Full => {
                rules.push((MatchRule::Exact(ustr(&value)), group.to_string()));
            }
        }
    }
    info!(target: "pasdns::router", "loaded geosite group '{tag}' with {count} rules");
}

pub fn load_geosite(path: &Path) -> Option<GeoSiteList> {
    let mut buf = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut f) => match f.read_to_end(&mut buf) {
            Ok(_) => {}
            Err(e) => {
                warn!(target: "pasdns::router", "failed to read {}: {e}", path.display());
                return None;
            }
        },
        Err(e) => {
            warn!(target: "pasdns::router", "geosite not found {}: {e}", path.display());
            return None;
        }
    }
    match decode_geosite(&buf) {
        Ok(list) => {
            let groups = list.entry.len();
            info!(target: "pasdns::router", "loaded {} with {groups} groups", path.display());
            Some(list)
        }
        Err(e) => {
            warn!(target: "pasdns::router", "failed to parse {}: {e}", path.display());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_router() -> Router {
        let routes = vec![
            RouteConfig {
                domains: vec!["example.com".to_string()],
                geosite: vec![],
                group: "test".to_string(),
            },
            RouteConfig {
                domains: vec!["*.google.com".to_string()],
                geosite: vec![],
                group: "google".to_string(),
            },
        ];
        Router::new(&routes, "default", None)
    }

    #[test]
    fn exact_match() {
        let router = make_router();
        assert_eq!(router.route("example.com"), "test");
    }

    #[test]
    fn suffix_match() {
        let router = make_router();
        assert_eq!(router.route("sub.example.com"), "test");
    }

    #[test]
    fn wildcard_match() {
        let router = make_router();
        assert_eq!(router.route("google.com"), "google");
        assert_eq!(router.route("www.google.com"), "google");
    }

    #[test]
    fn default_fallback() {
        let router = make_router();
        assert_eq!(router.route("unknown.com"), "default");
    }
}
