use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

#[derive(Clone)]
pub struct AddressFilter {
    allows: Arc<dyn Fn(SocketAddrV4) -> bool + Send + Sync>,
}

impl AddressFilter {
    pub fn strict() -> AddressFilter {
        AddressFilter {
            allows: Arc::new(strict_allows),
        }
    }

    #[doc(hidden)]
    pub fn permissive_for_tests() -> AddressFilter {
        AddressFilter {
            allows: Arc::new(|_| true),
        }
    }

    pub fn allows(&self, addr: SocketAddrV4) -> bool {
        (self.allows)(addr)
    }

    pub fn allows_socket(&self, addr: SocketAddr) -> bool {
        match addr {
            SocketAddr::V4(v4) => (self.allows)(v4),
            SocketAddr::V6(_) => false,
        }
    }
}

pub fn strict_allows(addr: SocketAddrV4) -> bool {
    if addr.port() == 0 {
        return false;
    }
    let ip: Ipv4Addr = *addr.ip();
    !(ip.is_unspecified() || ip.is_loopback() || ip.is_broadcast() || ip.is_multicast())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(ip: [u8; 4], port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::from(ip), port)
    }

    #[test]
    fn strict_drops_port_zero() {
        assert!(!AddressFilter::strict().allows(addr([93, 184, 216, 34], 0)));
    }

    #[test]
    fn strict_drops_unspecified_loopback_broadcast_and_multicast() {
        let filter = AddressFilter::strict();
        assert!(!filter.allows(addr([0, 0, 0, 0], 6881)));
        assert!(!filter.allows(addr([127, 0, 0, 1], 6881)));
        assert!(!filter.allows(addr([255, 255, 255, 255], 6881)));
        assert!(!filter.allows(addr([224, 0, 0, 1], 6881)));
    }

    #[test]
    fn strict_keeps_lan_and_public_ranges() {
        let filter = AddressFilter::strict();
        assert!(filter.allows(addr([10, 0, 0, 5], 6881)));
        assert!(filter.allows(addr([192, 168, 1, 20], 6881)));
        assert!(filter.allows(addr([172, 16, 4, 9], 6881)));
        assert!(filter.allows(addr([93, 184, 216, 34], 6881)));
    }

    #[test]
    fn permissive_variant_keeps_loopback_for_loopback_fakes() {
        let filter = AddressFilter::permissive_for_tests();
        assert!(filter.allows(addr([127, 0, 0, 1], 6881)));
    }

    #[test]
    fn socket_helper_rejects_ipv6_and_delegates_ipv4() {
        let strict = AddressFilter::strict();
        let v6: SocketAddr = "[::1]:6881".parse().unwrap();
        assert!(!strict.allows_socket(v6), "the dht service is ipv4 only");
        let v4: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        assert!(!strict.allows_socket(v4));
        let lan: SocketAddr = "192.168.1.20:6881".parse().unwrap();
        assert!(strict.allows_socket(lan));
    }
}
