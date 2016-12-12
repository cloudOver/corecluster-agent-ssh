MODULE = {
    'agents': [
        {'type': 'image', 'module': 'corecluster-storage-ssh.agents.image_ssh', 'count': 4},
        {'type': 'node', 'module': 'corecluster-storage-ssh.agents.node_ssh', 'count': 4},
        {'type': 'storage', 'module': 'corecluster-storage-ssh.agents.storage_ssh', 'count': 4},
    ],
}
