//go:build e2e
// +build e2e

package e2e

// This file is the single source of truth for the Kubernetes resource shapes and
// MySQL tuning the e2e Clusters use. Keeping them here, rather than re-literalled
// per manifest, lets the suite be right-sized for the constrained CI runner
// (8 vCPU / 16 GiB) in one place.
//
// See design/025-e2e-testing-overhaul.md.

// e2eInstanceResources is the resources block applied to instance Pods in e2e
// Clusters, sized so several multi-instance clusters can coexist on the
// single-node test cluster.
const e2eInstanceResources = `  resources:
    requests:
      cpu: 100m
      memory: 384Mi
    limits:
      cpu: "1"
      memory: 1536Mi
`

// e2eMySQLParameters is the my.cnf tuning applied to e2e Clusters, kept small so
// many instances fit on one node.
const e2eMySQLParameters = `    parameters:
      innodb_buffer_pool_size: 128M
      max_connections: "80"
`
