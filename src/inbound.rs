use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use governor::{
    DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota, RateLimiter,
    clock::{Clock, DefaultClock},
};
use parking_lot::RwLock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::InboundLimiterConfig;

/// IPv6 clients are aggregated by this prefix length for per-IP limiting. A /64
/// is the smallest allocation normally handed to a single subscriber, so a
/// client with a rotating interface identifier cannot mint unlimited buckets.
pub const DEFAULT_V6_PREFIX_LEN: u8 = 64;

/// Hard ceiling on the number of tracked per-IP buckets. Once reached, stale
/// buckets are evicted; if the state is still full, only a fraction of the
/// sharded keyed state is reset so existing budgets are preserved.
pub const DEFAULT_MAX_TRACKED_IPS: usize = 100_000;

/// The keyed limiter is sharded so eviction resets at most one shard (a
/// fraction of tracked budgets) instead of clearing every client's state.
const LIMITER_SHARDS: usize = 16;

pub const TRUSTED_PROXIES_ENV: &str = "PROVIDARR_INBOUND_TRUSTED_PROXIES";
pub const V6_PREFIX_ENV: &str = "PROVIDARR_INBOUND_V6_PREFIX_LEN";
pub const MAX_TRACKED_IPS_ENV: &str = "PROVIDARR_INBOUND_MAX_TRACKED_IPS";

/// A single-address, CIDR, or IP-range-style bypass rule parsed from config/env.
///
/// Supports `10.0.0.5`, `10.0.0.0/24`, `2001:db8::/32`, and bare IPv6 addresses.
/// IPv4-mapped IPv6 rules (e.g. `::ffff:10.0.0.0/120`) are normalised to their
/// IPv4 equivalent at parse time so they still match clients that are collapsed
/// to IPv4 before comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpRule {
    V4 { net: u32, prefix: u8 },
    V6 { net: u128, prefix: u8 },
}

impl IpRule {
    pub fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }

        let (addr, prefix) = match raw.split_once('/') {
            Some((addr, prefix)) => (addr.trim(), Some(prefix.trim().parse::<u8>().ok()?)),
            None => (raw, None),
        };

        if let Ok(v4) = addr.parse::<Ipv4Addr>() {
            let prefix = prefix.unwrap_or(32);
            if prefix > 32 {
                return None;
            }
            let bits = u32::from(v4);
            return Some(IpRule::V4 {
                net: bits & mask_v4(prefix),
                prefix,
            });
        }

        if let Ok(v6) = addr.parse::<Ipv6Addr>() {
            // Collapse `::ffff:a.b.c.d/N` to IPv4. The mapped prefix occupies the
            // top 96 bits, so the IPv4 prefix is `N - 96`. A prefix below 96 (or
            // above 128) cannot be expressed as an IPv4 prefix and would otherwise
            // underflow to a catch-all `0.0.0.0/0`, so reject it.
            if let Some(mapped) = v6.to_ipv4_mapped() {
                let prefix = prefix.unwrap_or(128);
                if !(96..=128).contains(&prefix) {
                    tracing::warn!(
                        rule = %addr,
                        prefix,
                        "ignoring IPv4-mapped IPv6 rule with out-of-range prefix"
                    );
                    return None;
                }
                let v4_prefix = prefix - 96;
                let bits = u32::from(mapped);
                return Some(IpRule::V4 {
                    net: bits & mask_v4(v4_prefix),
                    prefix: v4_prefix,
                });
            }

            let prefix = prefix.unwrap_or(128);
            if prefix > 128 {
                return None;
            }
            let bits = u128::from(v6);
            return Some(IpRule::V6 {
                net: bits & mask_v6(prefix),
                prefix,
            });
        }

        None
    }

    pub fn matches(&self, ip: IpAddr) -> bool {
        match (self, normalize_ip(ip)) {
            (IpRule::V4 { net, prefix }, IpAddr::V4(v4)) => {
                (u32::from(v4) & mask_v4(*prefix)) == *net
            }
            (IpRule::V6 { net, prefix }, IpAddr::V6(v6)) => {
                (u128::from(v6) & mask_v6(*prefix)) == *net
            }
            _ => false,
        }
    }
}

fn mask_v4(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn mask_v6(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    }
}

/// Collapses IPv4-mapped IPv6 addresses (`::ffff:1.2.3.4`) to IPv4 so bypass rules
/// written for either family behave predictably.
pub fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        IpAddr::V4(v4) => IpAddr::V4(v4),
    }
}

/// Parses a single token from an `X-Forwarded-For` chain. Accepts a bare IP, an
/// `ip:port` pair, a bracketed IPv6 address (`[::1]` / `[::1]:443`), and an IPv6
/// zone suffix (`fe80::1%eth0`). Returns `None` for anything unparseable.
fn parse_forwarded_hop(raw: &str) -> Option<IpAddr> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    // Bracketed IPv6, optionally followed by a port: `[::1]` or `[::1]:443`.
    if let Some(rest) = raw.strip_prefix('[') {
        let (addr, _port) = rest.split_once(']')?;
        return parse_ip_without_zone(addr.trim());
    }

    if let Some(ip) = parse_ip_without_zone(raw) {
        return Some(ip);
    }

    // `ip:port`. IPv6 addresses must be bracketed, so an unbracketed value with a
    // single trailing `:port` is an IPv4 address.
    match raw.rsplit_once(':') {
        Some((addr, port)) if port.parse::<u16>().is_ok() => parse_ip_without_zone(addr.trim()),
        _ => None,
    }
}

/// Strips an optional `%zone` suffix and parses the remaining address.
fn parse_ip_without_zone(raw: &str) -> Option<IpAddr> {
    let addr = raw.split('%').next().unwrap_or(raw).trim();
    addr.parse::<IpAddr>().ok()
}

/// Parses a whole `X-Forwarded-For` value. Returns `None` if any non-empty hop
/// fails to parse, so the caller can fail closed rather than silently dropping
/// individual hops and promoting an attacker-supplied value to the client.
fn parse_forwarded_chain(raw: &str) -> Option<Vec<IpAddr>> {
    let mut chain = Vec::new();
    for hop in raw.split(',') {
        let hop = hop.trim();
        if hop.is_empty() {
            continue;
        }
        chain.push(normalize_ip(parse_forwarded_hop(hop)?));
    }
    Some(chain)
}

/// Parses configured IP rules, warning about (and dropping) entries that are not
/// valid addresses or CIDRs instead of silently ignoring them.
fn parse_rules(raw: &[String], kind: &str) -> Vec<IpRule> {
    raw.iter()
        .filter_map(|entry| match IpRule::parse(entry) {
            // A `/0` rule matches every address. In the bypass list that disables
            // limiting entirely; in the trusted-proxy list it makes every peer a
            // trusted proxy. Drop it so a single typo cannot open the door.
            Some(IpRule::V4 { prefix: 0, .. } | IpRule::V6 { prefix: 0, .. }) => {
                tracing::warn!(
                    entry = %entry,
                    kind,
                    "ignoring catch-all inbound IP rule (/0 would disable limiting)"
                );
                None
            }
            Some(rule) => Some(rule),
            None => {
                tracing::warn!(entry = %entry, kind, "ignoring invalid inbound IP rule");
                None
            }
        })
        .collect()
}

fn env_rules(name: &str, kind: &str) -> Vec<IpRule> {
    match std::env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => {
            let entries: Vec<String> = raw
                .split(',')
                .map(|entry| entry.trim().to_string())
                .filter(|entry| !entry.is_empty())
                .collect();
            parse_rules(&entries, kind)
        }
        _ => Vec::new(),
    }
}

fn env_v6_prefix_len() -> u8 {
    std::env::var(V6_PREFIX_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u8>().ok())
        .filter(|prefix| *prefix <= 128)
        .unwrap_or(DEFAULT_V6_PREFIX_LEN)
}

fn env_max_tracked_ips() -> usize {
    std::env::var(MAX_TRACKED_IPS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|max| *max > 0)
        .unwrap_or(DEFAULT_MAX_TRACKED_IPS)
}

/// Per-client-IP rate limiter for requests coming *into* Providarr (distinct from
/// the per-provider limiter that throttles outbound calls).
pub struct InboundLimiter {
    enabled: bool,
    trust_forwarded_for: bool,
    /// Immediate TCP peers allowed to supply `X-Forwarded-For` / `X-Real-IP`.
    trusted_proxies: Vec<IpRule>,
    bypass: Vec<IpRule>,
    v6_prefix_len: u8,
    max_tracked_ips: usize,
    quota: Quota,
    limiters: Vec<RwLock<DefaultKeyedRateLimiter<IpAddr>>>,
    evict_cursor: AtomicUsize,
    global: Option<DefaultDirectRateLimiter>,
    concurrency: Option<Arc<Semaphore>>,
}

impl InboundLimiter {
    pub fn new(config: &InboundLimiterConfig) -> Self {
        let trusted = env_rules(TRUSTED_PROXIES_ENV, "trusted proxy");
        Self::build(config, trusted)
    }

    /// Constructs a limiter with an explicit trusted-proxy set. Used by tests and
    /// embedders; the running service uses [`InboundLimiter::new`].
    pub fn with_trusted_proxies(config: &InboundLimiterConfig, trusted: Vec<IpRule>) -> Self {
        Self::build(config, trusted)
    }

    fn build(config: &InboundLimiterConfig, trusted_proxies: Vec<IpRule>) -> Self {
        let global = (config.global_requests_per_second > 0.0).then(|| {
            RateLimiter::direct(quota_for(
                config.global_requests_per_second,
                config.global_burst.max(1),
            ))
        });

        let concurrency = (config.max_concurrent > 0)
            .then(|| Arc::new(Semaphore::new(config.max_concurrent as usize)));

        let quota = quota_for(config.requests_per_second, config.burst);
        let limiters = (0..LIMITER_SHARDS)
            .map(|_| RwLock::new(RateLimiter::keyed(quota)))
            .collect();

        if config.trust_forwarded_for && trusted_proxies.is_empty() {
            tracing::warn!(
                env = TRUSTED_PROXIES_ENV,
                "inbound.trust_forwarded_for is enabled but no trusted proxies are configured; \
                 X-Forwarded-For / X-Real-IP will be ignored and the TCP peer used"
            );
        }

        Self {
            enabled: config.enabled,
            trust_forwarded_for: config.trust_forwarded_for,
            trusted_proxies,
            bypass: parse_rules(&config.bypass, "bypass"),
            v6_prefix_len: env_v6_prefix_len(),
            max_tracked_ips: env_max_tracked_ips(),
            quota,
            limiters,
            evict_cursor: AtomicUsize::new(0),
            global,
            concurrency,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn trust_forwarded_for(&self) -> bool {
        self.trust_forwarded_for
    }

    pub fn bypass_count(&self) -> usize {
        self.bypass.len()
    }

    pub fn trusted_proxy_count(&self) -> usize {
        self.trusted_proxies.len()
    }

    /// Number of per-IP buckets currently tracked.
    pub fn tracked_keys(&self) -> usize {
        self.limiters.iter().map(|shard| shard.read().len()).sum()
    }

    /// Selects the shard that owns `key`, so a given client always maps to one
    /// keyed limiter and eviction only ever touches a fraction of the state.
    fn shard_for(&self, key: &IpAddr) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.limiters.len()
    }

    /// True when the address matches a configured bypass IP/CIDR.
    pub fn is_bypassed(&self, ip: IpAddr) -> bool {
        let ip = normalize_ip(ip);
        self.bypass.iter().any(|rule| rule.matches(ip))
    }

    /// True when `ip` is an immediate proxy allowed to set forwarding headers.
    pub fn is_trusted_proxy(&self, ip: IpAddr) -> bool {
        let ip = normalize_ip(ip);
        self.trusted_proxies.iter().any(|rule| rule.matches(ip))
    }

    /// Resolves the real client IP given the immediate TCP peer and forwarding
    /// headers. Forwarded headers are only honoured when the peer is a configured
    /// trusted proxy; the real client is then the rightmost hop in the chain that
    /// is not itself a trusted proxy, falling back to the peer.
    pub fn resolve_client_ip(
        &self,
        peer: IpAddr,
        forwarded_for: Option<&str>,
        real_ip: Option<&str>,
    ) -> IpAddr {
        let peer = normalize_ip(peer);
        if !self.trust_forwarded_for || !self.is_trusted_proxy(peer) {
            return peer;
        }

        if let Some(raw) = forwarded_for {
            match parse_forwarded_chain(raw) {
                // Walk from the trusted edge inward: the first hop we do not trust
                // is the closest thing to the real client.
                Some(chain) if !chain.is_empty() => {
                    return match chain.iter().rev().find(|hop| !self.is_trusted_proxy(**hop)) {
                        Some(client) => *client,
                        // Every hop (and the peer) is a trusted proxy. Never fall
                        // back to the attacker-controlled leftmost hop; use the
                        // immediate TCP peer instead.
                        None => peer,
                    };
                }
                // An empty header carries no information; fall through to
                // X-Real-IP.
                Some(_) => {}
                // Any unparseable hop invalidates the whole chain: silently
                // dropping it could promote a spoofed left value to the client.
                None => {
                    tracing::warn!("ignoring malformed X-Forwarded-For header; using TCP peer");
                    return peer;
                }
            }
        }

        if let Some(raw) = real_ip
            && let Ok(ip) = raw.trim().parse::<IpAddr>()
        {
            return normalize_ip(ip);
        }

        peer
    }

    /// Maps a client address to its limiter key, aggregating IPv6 clients by the
    /// configured prefix (default /64) so a single subscriber cannot rotate the
    /// low bits to evade the per-IP budget.
    fn key_for(&self, ip: IpAddr) -> IpAddr {
        match normalize_ip(ip) {
            IpAddr::V4(v4) => IpAddr::V4(v4),
            IpAddr::V6(v6) => {
                let bits = u128::from(v6) & mask_v6(self.v6_prefix_len);
                IpAddr::V6(Ipv6Addr::from(bits))
            }
        }
    }

    /// Returns `Ok(())` if allowed (or bypassed), or the time to wait before retrying.
    pub fn check(&self, ip: IpAddr) -> Result<(), Duration> {
        let ip = normalize_ip(ip);
        if self.is_bypassed(ip) {
            return Ok(());
        }

        let key = self.key_for(ip);
        let shard = self.shard_for(&key);
        self.evict_if_needed(shard);

        self.limiters[shard]
            .read()
            .check_key(&key)
            .map(|_| ())
            .map_err(|not_until| not_until.wait_time_from(DefaultClock::default().now()))
    }

    /// Server-wide ceiling across all client IPs. `Ok(())` when allowed or disabled,
    /// otherwise the time to wait before retrying.
    pub fn check_global(&self) -> Result<(), Duration> {
        match &self.global {
            Some(limiter) => limiter
                .check()
                .map(|_| ())
                .map_err(|not_until| not_until.wait_time_from(DefaultClock::default().now())),
            None => Ok(()),
        }
    }

    /// Acquires an in-flight slot, or reports that the server is at capacity.
    /// The returned permit must be held for the duration of the request.
    pub fn acquire_concurrency(&self) -> Result<Option<OwnedSemaphorePermit>, Duration> {
        match &self.concurrency {
            Some(semaphore) => semaphore
                .clone()
                .try_acquire_owned()
                .map(Some)
                .map_err(|_| Duration::from_secs(1)),
            None => Ok(None),
        }
    }

    /// Drops per-IP limiter state for addresses not seen recently, bounding memory
    /// when the endpoint is exposed to a large, changing set of client IPs.
    pub fn retain_recent(&self) {
        for shard in &self.limiters {
            shard.read().retain_recent();
        }
    }

    /// Keeps the keyed state bounded. When the tracked-key count reaches the cap,
    /// stale buckets are dropped first; if the state is still full, only a subset
    /// of shards is reset, so a burst of new clients cannot clear every existing
    /// client's budget at once.
    fn evict_if_needed(&self, protected_shard: usize) {
        if self.tracked_keys() < self.max_tracked_ips {
            return;
        }

        for shard in &self.limiters {
            shard.read().retain_recent();
        }

        let shards = self.limiters.len();
        let mut reset = 0;
        while self.tracked_keys() >= self.max_tracked_ips && reset < shards {
            // Rotate through the shards, avoiding the caller's own shard first so
            // a client cannot trigger an eviction that refreshes its own budget.
            let mut victim = self.evict_cursor.fetch_add(1, Ordering::Relaxed) % shards;
            if victim == protected_shard {
                victim = (victim + 1) % shards;
            }

            let mut shard = self.limiters[victim].write();
            if shard.is_empty() {
                reset += 1;
                continue;
            }
            tracing::warn!(
                max_tracked_ips = self.max_tracked_ips,
                shard = victim,
                "inbound per-IP state reached its bound; evicting one shard of tracked keys"
            );
            *shard = RateLimiter::keyed(self.quota);
            reset += 1;
        }
    }
}

fn quota_for(requests_per_second: f64, burst: u32) -> Quota {
    let burst = NonZeroU32::new(burst.max(1)).expect("burst is non-zero");

    // Not a usable rate: treat the bucket as effectively unlimited.
    if !requests_per_second.is_finite() || requests_per_second <= 0.0 {
        return Quota::with_period(Duration::from_nanos(1))
            .expect("non-zero period")
            .allow_burst(NonZeroU32::new(u32::MAX).expect("non-zero"));
    }

    // `1 / rps` can overflow `Duration` for extremely small rates; `try_from`
    // (rather than the panicking `from_secs_f64`) keeps that safe.
    let period_secs = (1.0 / requests_per_second).max(1e-9);
    let period = Duration::try_from_secs_f64(period_secs).unwrap_or(Duration::from_secs(1));

    Quota::with_period(period)
        .expect("period is non-zero")
        .allow_burst(burst)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(rps: f64, burst: u32) -> InboundLimiterConfig {
        InboundLimiterConfig {
            enabled: true,
            requests_per_second: rps,
            burst,
            global_requests_per_second: 0.0,
            global_burst: 1,
            max_concurrent: 0,
            trust_forwarded_for: false,
            bypass: Vec::new(),
        }
    }

    #[test]
    fn parses_and_matches_cidr_rules() {
        let v4 = IpRule::parse("10.0.0.0/24").unwrap();
        assert!(v4.matches("10.0.0.1".parse().unwrap()));
        assert!(v4.matches("10.0.0.255".parse().unwrap()));
        assert!(!v4.matches("10.0.1.0".parse().unwrap()));

        let host = IpRule::parse("192.168.1.10").unwrap();
        assert!(host.matches("192.168.1.10".parse().unwrap()));
        assert!(!host.matches("192.168.1.11".parse().unwrap()));

        let v6 = IpRule::parse("2001:db8::/32").unwrap();
        assert!(v6.matches("2001:db8:1234::1".parse().unwrap()));
        assert!(!v6.matches("2001:db9::1".parse().unwrap()));

        assert!(IpRule::parse("not-an-ip").is_none());
        assert!(IpRule::parse("10.0.0.0/33").is_none());
    }

    #[test]
    fn normalizes_ipv4_mapped_rules_at_parse_time() {
        let mapped = IpRule::parse("::ffff:10.0.0.0/120").unwrap();
        assert!(mapped.matches("10.0.0.5".parse().unwrap()));
        assert!(mapped.matches("::ffff:10.0.0.5".parse().unwrap()));
        assert!(!mapped.matches("10.0.1.5".parse().unwrap()));

        // A bare mapped address becomes the equivalent host rule.
        let host = IpRule::parse("::ffff:192.168.1.7").unwrap();
        assert!(host.matches("192.168.1.7".parse().unwrap()));
    }

    #[test]
    fn invalid_rules_are_dropped_without_panicking() {
        let mut cfg = config(1.0, 1);
        cfg.bypass = vec!["10.0.0.0/24".to_string(), "garbage".to_string()];
        let limiter = InboundLimiter::new(&cfg);
        assert_eq!(limiter.bypass_count(), 1);
        assert!(limiter.is_bypassed("10.0.0.5".parse().unwrap()));
    }

    #[test]
    fn bypasses_matching_ips() {
        let mut cfg = config(1.0, 1);
        cfg.bypass = vec!["127.0.0.1".to_string(), "10.0.0.0/24".to_string()];
        let limiter = InboundLimiter::new(&cfg);

        // Bypassed addresses are never limited, even beyond the burst.
        for _ in 0..10 {
            assert!(limiter.check("10.0.0.42".parse().unwrap()).is_ok());
            assert!(limiter.check("127.0.0.1".parse().unwrap()).is_ok());
        }

        assert!(limiter.is_bypassed("10.0.0.7".parse().unwrap()));
        assert!(!limiter.is_bypassed("10.0.1.7".parse().unwrap()));
    }

    #[test]
    fn bypass_handles_ipv4_mapped_ipv6() {
        let mut cfg = config(1.0, 1);
        cfg.bypass = vec!["10.0.0.0/24".to_string()];
        let limiter = InboundLimiter::new(&cfg);

        assert!(limiter.is_bypassed("::ffff:10.0.0.5".parse().unwrap()));
    }

    #[test]
    fn limits_per_ip_without_affecting_others() {
        let limiter = InboundLimiter::new(&config(1.0, 2));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();

        assert!(limiter.check(a).is_ok());
        assert!(limiter.check(a).is_ok());
        let wait = limiter
            .check(a)
            .expect_err("third immediate call is limited");
        assert!(wait > Duration::ZERO);

        assert!(limiter.check(b).is_ok(), "other IPs are unaffected");
    }

    #[test]
    fn aggregates_ipv6_clients_by_slash_64() {
        let limiter = InboundLimiter::new(&config(1.0, 2));

        assert!(limiter.check("2001:db8:1:2::1".parse().unwrap()).is_ok());
        assert!(limiter.check("2001:db8:1:2::2".parse().unwrap()).is_ok());
        assert!(
            limiter.check("2001:db8:1:2::3".parse().unwrap()).is_err(),
            "addresses in the same /64 share one bucket"
        );

        assert!(
            limiter.check("2001:db8:1:3::1".parse().unwrap()).is_ok(),
            "a different /64 gets its own bucket"
        );
    }

    #[test]
    fn resolves_rightmost_untrusted_hop_only_beneath_trusted_proxies() {
        let mut limiter = InboundLimiter::new(&config(1.0, 1));
        limiter.trust_forwarded_for = true;
        limiter.trusted_proxies = vec![IpRule::parse("10.0.0.0/8").unwrap()];

        // Trusted peer, trusted intermediate hop: use the rightmost untrusted hop.
        let client = limiter.resolve_client_ip(
            "10.0.0.1".parse().unwrap(),
            Some("203.0.113.9, 10.0.0.2"),
            None,
        );
        assert_eq!(client, "203.0.113.9".parse::<IpAddr>().unwrap());

        // Untrusted peer: forwarded headers are ignored entirely.
        let spoofed = limiter.resolve_client_ip(
            "203.0.113.5".parse().unwrap(),
            Some("1.2.3.4"),
            Some("5.6.7.8"),
        );
        assert_eq!(spoofed, "203.0.113.5".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn malformed_forwarded_chain_fails_closed_to_peer() {
        let mut limiter = InboundLimiter::new(&config(1.0, 1));
        limiter.trust_forwarded_for = true;
        limiter.trusted_proxies = vec![IpRule::parse("10.0.0.0/8").unwrap()];
        let peer: IpAddr = "10.0.0.1".parse().unwrap();

        // A spoofed leftmost value followed by an unparseable hop must not become
        // the client key; the whole header is discarded in favour of the peer.
        let client =
            limiter.resolve_client_ip(peer, Some("203.0.113.9, not-an-ip"), Some("198.51.100.7"));
        assert_eq!(client, peer);
    }

    #[test]
    fn parses_forwarded_hops_with_ports_brackets_and_zones() {
        let mut limiter = InboundLimiter::new(&config(1.0, 1));
        limiter.trust_forwarded_for = true;
        limiter.trusted_proxies = vec![IpRule::parse("10.0.0.0/8").unwrap()];
        let peer: IpAddr = "10.0.0.1".parse().unwrap();

        assert_eq!(
            limiter.resolve_client_ip(peer, Some("203.0.113.9:1234"), None),
            "203.0.113.9".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            limiter.resolve_client_ip(peer, Some("[2001:db8::5]:443"), None),
            "2001:db8::5".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            limiter.resolve_client_ip(peer, Some("fe80::1%eth0"), None),
            "fe80::1".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            limiter.resolve_client_ip(peer, Some("[2001:db8::6]"), None),
            "2001:db8::6".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn all_trusted_hops_fall_back_to_peer_not_leftmost() {
        let mut limiter = InboundLimiter::new(&config(1.0, 1));
        limiter.trust_forwarded_for = true;
        limiter.trusted_proxies = vec![IpRule::parse("10.0.0.0/8").unwrap()];
        let peer: IpAddr = "10.0.0.1".parse().unwrap();

        let client = limiter.resolve_client_ip(peer, Some("10.0.0.9, 10.0.0.8"), None);
        assert_eq!(
            client, peer,
            "must not use the leftmost attacker-supplied hop"
        );
    }

    #[test]
    fn rejects_ipv4_mapped_cidr_underflow() {
        // A prefix below 96 would underflow to a catch-all IPv4 rule.
        assert!(IpRule::parse("::ffff:10.0.0.0/90").is_none());
        assert!(IpRule::parse("::ffff:10.0.0.0/0").is_none());
        // Above the valid IPv6 prefix length.
        assert!(IpRule::parse("::ffff:10.0.0.0/129").is_none());
    }

    #[test]
    fn rejects_catch_all_rules_for_all_lists() {
        let rules = parse_rules(
            &[
                "0.0.0.0/0".to_string(),
                "::/0".to_string(),
                "10.0.0.0/8".to_string(),
            ],
            "test",
        );
        assert_eq!(rules.len(), 1, "catch-all rules are dropped");
        assert!(rules[0].matches("10.0.0.1".parse().unwrap()));
    }

    #[test]
    fn eviction_resets_only_a_subset_of_shards() {
        let mut limiter = InboundLimiter::new(&config(1.0, 1));
        limiter.max_tracked_ips = 2;

        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        limiter.limiters[0].write().check_key(&a).unwrap();
        limiter.limiters[1].write().check_key(&b).unwrap();
        assert_eq!(limiter.tracked_keys(), 2);

        limiter.evict_if_needed(LIMITER_SHARDS - 1);

        assert_eq!(
            limiter.tracked_keys(),
            1,
            "only one shard is reset, so other budgets survive"
        );
    }

    #[test]
    fn global_limiter_rejects_across_distinct_ips() {
        let mut cfg = config(1000.0, 1000);
        cfg.global_requests_per_second = 1.0;
        cfg.global_burst = 2;
        let limiter = InboundLimiter::new(&cfg);

        // Per-IP buckets are wide open, but the global cap still trips from new IPs.
        assert!(limiter.check_global().is_ok());
        assert!(limiter.check_global().is_ok());
        assert!(
            limiter.check_global().is_err(),
            "third request is rejected server-wide"
        );
    }

    #[test]
    fn global_limiter_disabled_when_rps_is_zero() {
        let limiter = InboundLimiter::new(&config(1.0, 1));
        for _ in 0..50 {
            assert!(limiter.check_global().is_ok());
        }
    }

    #[test]
    fn concurrency_cap_is_reported_and_permit_releases() {
        let mut cfg = config(1000.0, 1000);
        cfg.max_concurrent = 1;
        let limiter = InboundLimiter::new(&cfg);

        let permit = limiter
            .acquire_concurrency()
            .expect("first acquisition succeeds")
            .expect("cap configured");

        assert!(
            limiter.acquire_concurrency().is_err(),
            "second concurrent request is rejected while the permit is held"
        );

        drop(permit);

        assert!(
            limiter.acquire_concurrency().is_ok(),
            "slot is released when the permit is dropped"
        );
    }

    #[test]
    fn retain_recent_is_safe_to_call() {
        let limiter = InboundLimiter::new(&config(1.0, 1));
        let _ = limiter.check("10.0.0.1".parse().unwrap());
        limiter.retain_recent();
    }

    #[test]
    fn quota_for_survives_extreme_rates() {
        for rps in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e-300, f64::MAX] {
            let _ = quota_for(rps, 1);
        }
    }

    #[test]
    fn reports_enabled_state() {
        let mut cfg = config(10.0, 10);
        cfg.enabled = false;
        assert!(!InboundLimiter::new(&cfg).enabled());
    }
}
