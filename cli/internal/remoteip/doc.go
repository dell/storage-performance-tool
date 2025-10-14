/*
Package remoteip provides helpers to determine a routable IP address on a host
to advertise for Java RMI when launching remote Spt containers.

It executes a small sequence of shell commands via the shared command executor
against the target host to find the default-route source IP or a suitable
non-docker, non-loopback IPv4 address, falling back to common hostname-based
lookups. The first valid IP discovered is returned.
*/
package remoteip
