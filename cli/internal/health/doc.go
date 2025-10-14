/*
Package health provides lightweight HTTP probes for Spt nodes, used by
remote orchestration to verify readiness (/run) and to fetch metrics
(/metrics/json). These helpers are transport-agnostic and operate on a baseURL
string such as "http://host:9999".
*/
package health
