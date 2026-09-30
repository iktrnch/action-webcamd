use std::net::{IpAddr, Ipv4Addr};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, TryStreamExt};
use netlink_packet_core::{NetlinkMessage, NetlinkPayload};
use netlink_packet_route::{RouteNetlinkMessage, address::AddressAttribute};
use netlink_sys::SocketAddr;
use rtnetlink::{Handle, MulticastGroup, new_multicast_connection};

/// A native netlink watcher for IPv4 address changes. The udev layer decides
/// which interface belongs to a GoPro; this component only reports addresses
/// for a supplied interface index.
pub(crate) struct AddressMonitor {
    handle: Handle,
    messages:
        futures_channel::mpsc::UnboundedReceiver<(NetlinkMessage<RouteNetlinkMessage>, SocketAddr)>,
}

impl AddressMonitor {
    /// Opens the IPv4-address multicast group before device reconciliation so
    /// an address assignment cannot be missed between startup and monitoring.
    pub(crate) fn open() -> Result<Self> {
        let (connection, handle, messages) =
            new_multicast_connection(&[MulticastGroup::Ipv4Ifaddr])
                .context("failed to open IPv4 netlink address monitor")?;
        tokio::spawn(connection);

        Ok(Self { handle, messages })
    }

    /// Returns the current usable IPv4 addresses for one kernel interface.
    pub(crate) async fn addresses_for(&self, ifindex: u32) -> Result<Vec<Ipv4Addr>> {
        let messages = self
            .handle
            .address()
            .get()
            .set_link_index_filter(ifindex)
            .execute()
            .try_collect::<Vec<_>>()
            .await
            .with_context(|| {
                format!("failed to enumerate IPv4 addresses for interface index {ifindex}")
            })?;

        Ok(messages
            .iter()
            .filter_map(address_from_message)
            .filter(is_usable_ipv4)
            .collect())
    }

    /// Waits for the next IPv4 address add or removal notification. Messages
    /// outside the IPv4 address family are ignored by the subscription.
    pub(crate) async fn next_change(&mut self) -> Result<AddressChange> {
        while let Some((message, _)) = self.messages.next().await {
            let NetlinkPayload::InnerMessage(
                RouteNetlinkMessage::NewAddress(address) | RouteNetlinkMessage::DelAddress(address),
            ) = message.payload
            else {
                continue;
            };

            let Some(host_address) = address_from_message(&address) else {
                continue;
            };
            if !is_usable_ipv4(&host_address) {
                continue;
            }

            return Ok(AddressChange {
                ifindex: address.header.index,
                address: host_address,
            });
        }

        bail!("IPv4 netlink address monitor ended unexpectedly")
    }
}

/// A changed IPv4 address; callers re-query the interface so both address
/// additions and removals are represented by one consistent inventory update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AddressChange {
    pub(crate) ifindex: u32,
    pub(crate) address: Ipv4Addr,
}

fn address_from_message(
    message: &netlink_packet_route::address::AddressMessage,
) -> Option<Ipv4Addr> {
    message
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            AddressAttribute::Local(IpAddr::V4(address))
            | AddressAttribute::Address(IpAddr::V4(address)) => Some(*address),
            _ => None,
        })
}

fn is_usable_ipv4(address: &Ipv4Addr) -> bool {
    !address.is_unspecified()
        && !address.is_loopback()
        && !address.is_multicast()
        && !address.is_broadcast()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_non_host_ipv4_addresses() {
        assert!(is_usable_ipv4(&Ipv4Addr::new(172, 27, 187, 52)));
        assert!(!is_usable_ipv4(&Ipv4Addr::UNSPECIFIED));
        assert!(!is_usable_ipv4(&Ipv4Addr::LOCALHOST));
        assert!(!is_usable_ipv4(&Ipv4Addr::new(224, 0, 0, 1)));
        assert!(!is_usable_ipv4(&Ipv4Addr::BROADCAST));
    }
}
