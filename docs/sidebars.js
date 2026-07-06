// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    {
      type: 'doc',
      id: 'index',
      label: 'Overview',
    },
    {
      type: 'html',
      value: '<small class="sidebar-section-label">Getting Started</small>',
      defaultStyle: true,
    },
    {
      type: 'doc',
      id: 'quickstart',
      label: 'Quickstart',
    },
    {
      type: 'doc',
      id: 'cluster-lifecycle',
      label: 'Cluster Lifecycle',
    },
    {
      type: 'doc',
      id: 'instance-images',
      label: 'Instance Images and Versions',
    },
    {
      type: 'doc',
      id: 'mariadb',
      label: 'MariaDB Flavor',
    },
    {
      type: 'doc',
      id: 'storage',
      label: 'Storage',
    },
    {
      type: 'html',
      value: '<small class="sidebar-section-label">Core Design</small>',
      defaultStyle: true,
    },
    {
      type: 'doc',
      id: 'replication-failover',
      label: 'Replication and Failover',
    },
    {
      type: 'doc',
      id: 'group-replication',
      label: 'Group Replication',
    },
    {
      type: 'doc',
      id: 'security-model',
      label: 'Security Model',
    },
    {
      type: 'doc',
      id: 'multi-tenancy',
      label: 'Multi-Tenancy',
    },
    {
      type: 'doc',
      id: 'database-users',
      label: 'Database Users',
    },
    {
      type: 'doc',
      id: 'deployment-modes',
      label: 'Deployment Modes',
    },
    {
      type: 'doc',
      id: 'operator-upgrades',
      label: 'Operator Upgrades',
    },
    {
      type: 'doc',
      id: 'major-version-upgrade',
      label: 'MySQL Version Upgrades',
    },
    {
      type: 'html',
      value: '<small class="sidebar-section-label">Backup and Recovery</small>',
      defaultStyle: true,
    },
    {
      type: 'doc',
      id: 'backup-recovery',
      label: 'Physical Backup and Recovery',
    },
    {
      type: 'doc',
      id: 'scheduled-backups',
      label: 'Scheduled Backups',
    },
    {
      type: 'doc',
      id: 'pitr',
      label: 'Point-In-Time Recovery',
    },
    {
      type: 'doc',
      id: 'pitr-internals',
      label: 'Binlog and PITR Internals',
    },
    {
      type: 'doc',
      id: 'backup-retention-deletion',
      label: 'Backup Retention and Deletion',
    },
    {
      type: 'doc',
      id: 'object-store',
      label: 'Object Store Configuration',
    },
    {
      type: 'html',
      value: '<small class="sidebar-section-label">Day-2 Operations</small>',
      defaultStyle: true,
    },
    {
      type: 'doc',
      id: 'operations',
      label: 'Operations Runbooks',
    },
    {
      type: 'doc',
      id: 'monitoring',
      label: 'Monitoring',
    },
    {
      type: 'doc',
      id: 'autoscaling',
      label: 'Autoscaling',
    },
    {
      type: 'doc',
      id: 'troubleshooting',
      label: 'Troubleshooting',
    },
    {
      type: 'html',
      value: '<small class="sidebar-section-label">Reference</small>',
      defaultStyle: true,
    },
    {
      type: 'doc',
      id: 'api-reference',
      label: 'API Reference',
    },
  ],
};

module.exports = sidebars;
