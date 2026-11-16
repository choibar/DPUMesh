#!/usr/bin/env python3
"""Print Kubernetes native gRPC fixtures; the node device plugin attests each Pod.

The client stays idle until measure.py starts channel-bench. The server opens
its own Pod address, so registration validates LISTEN without trusting a VIP.
"""
import argparse
import json


def objects(namespace, client_node, server_node, image, host_path, host_pci, cpus):
    result = [{'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace}}]
    for role, node in [('client', client_node), ('server', server_node)]:
        name = 'native-peer-' + role
        script = ('export DPUMESH_SERVICE="$DPUMESH_POD_IP:50051"; '
                  'exec taskset -c "$HOST_CPUS" /opt/native/bin/channel-bench -mode server'
                  if role == 'server' else 'exec sleep infinity')
        env = {'DPUMESH_PCI_ADDR': host_pci, 'DPUMESH_SERVER': 'DPUMesh0',
               'DPUMESH_SERVICE_NAME': name, 'DPUMESH_BACKEND_MAX': '16',
               'LD_LIBRARY_PATH': '/opt/native/lib:/opt/mellanox/doca/lib/x86_64-linux-gnu:/opt/mellanox/flexio/lib',
               'HOST_CPUS': cpus, 'GOMAXPROCS': '8'}
        result.append({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
            'name': name, 'namespace': namespace, 'labels': {'app': name},
            'annotations': {'linkerd.io/inject': 'disabled'}}, 'spec': {
            'nodeName': node, 'restartPolicy': 'Never', 'automountServiceAccountToken': False,
            'containers': [{'name': 'app', 'image': image, 'imagePullPolicy': 'Never',
                'command': ['/bin/bash', '-ec', script],
                'env': [{'name': k, 'value': v} for k, v in env.items()] + [
                    {'name': 'DPUMESH_POD_IP', 'valueFrom': {'fieldRef': {'fieldPath': 'status.podIP'}}}],
                'resources': {'requests': {'dpumesh.io/channel': 1},
                              'limits': {'dpumesh.io/channel': 1, 'memory': '4Gi'}},
                'securityContext': {'privileged': True, 'runAsUser': 0},
                'volumeMounts': [{'name': n, 'mountPath': p, 'readOnly': n != 'verbs'}
                                for n, p in [('native','/opt/native'),('sdk','/opt/mellanox'),('verbs','/dev/infiniband')]]}],
            'volumes': [{'name': n, 'hostPath': {'path': p, 'type': 'Directory'}}
                        for n,p in [('native',host_path),('sdk','/opt/mellanox'),('verbs','/dev/infiniband')]]}})
        # Ready membership binds the calling Pod to its Service; this is also
        # required for the client even though it exposes no TCP application port.
        result.append({'apiVersion': 'v1', 'kind': 'Service',
            'metadata': {'name': name, 'namespace': namespace}, 'spec': {
                'selector': {'app': name}, 'ports': [{'port': 50051, 'targetPort': 50051}]}})
    return {'apiVersion': 'v1', 'kind': 'List', 'items': result}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--namespace', default='dpumesh-native-peer')
    p.add_argument('--client-node', required=True)
    p.add_argument('--server-node', required=True)
    p.add_argument('--image', required=True)
    p.add_argument('--host-path', default='/opt/dpumesh-native-migration')
    p.add_argument('--host-pci', required=True)
    p.add_argument('--host-cpus', default='4-11')
    a = p.parse_args()
    print(json.dumps(objects(a.namespace,a.client_node,a.server_node,a.image,a.host_path,a.host_pci,a.host_cpus),indent=2))


if __name__ == '__main__':
    main()
