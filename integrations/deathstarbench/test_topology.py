import unittest
from topology import generate, PORTS

class TopologyTest(unittest.TestCase):
    def test_poll_mode_metadata_preserves_routing(self):
        busy=generate(4,{},1,'0b:00.1','0-15',4)
        event=generate(4,{},1,'0b:00.1','0-15',4,poll_mode='event')
        self.assertTrue(busy['busy_poll'])
        self.assertFalse(event['busy_poll'])
        self.assertEqual(event['poll_mode'],'event')
        self.assertEqual(busy['processes'],event['processes'])
        self.assertEqual(busy['services'],event['services'])
        with self.assertRaises(ValueError):generate(4,{},1,'0b:00.1','0-15',4,poll_mode='invalid')
    def test_bidirectional_budget_and_unique_identities(self):
        t=generate(1,{},1,'0b:00.1','0-11',4)
        self.assertEqual(t['worker_flow_budget'],[18])
        self.assertEqual(len({p['env']['DPUMESH_POD_IP'] for p in t['processes']}),10)
        s=next(p for p in t['processes'] if p['service']=='search')
        self.assertEqual(s['steady_flow_budget'],3)
    def test_refuses_oversubscription(self):
        with self.assertRaises(ValueError):generate(1,{},4,'0b:00.1','0-11',4)
    def test_replica_count_does_not_create_channels_in_one_process(self):
        t=generate(4,{'search':2},2,'0b:00.1','0-11',4)
        self.assertEqual(len(t['processes']),12)
        self.assertEqual(len(t['services']['srv-search']['tcp']),2)
    def test_stable_unique_replica_endpoints(self):
        a=generate(4,{},1,'0b:00.1','0-11',4)
        b=generate(4,{'geo':3},1,'0b:00.1','0-11',4)
        self.assertEqual(a['services']['srv-search']['endpoints'][0]['dma'], b['services']['srv-search']['endpoints'][0]['dma'])
        endpoints=[e['dma'] for s in b['services'].values() for e in s['endpoints']]
        self.assertEqual(len(set(endpoints)),len(endpoints))
        vips={f"{s['vip']}:{s['port']}" for s in b['services'].values()}
        self.assertFalse(set(endpoints)&vips)
        for p in b['processes']:
            if p['service'] in PORTS:self.assertIn(p['env']['DPUMESH_SERVICE'],endpoints)
    def test_unknown_service(self):
        with self.assertRaises(ValueError):generate(4,{'typo':2},1,'0b:00.1','0-11',4)
    def test_frontend_placement_preserves_backends_and_budgets(self):
        spec=dict(reservation=4,rate=4,search=4,profile=2,geo=2,recommendation=2)
        old=generate(16,spec,4,'0b:00.1','0-15',4)
        new=generate(16,spec,4,'0b:00.1','0-15',4,frontend_workers=[14,15,3,4])
        self.assertEqual(old['services'],new['services'])
        self.assertEqual(new['frontend_workers'],[14,15,3,4])
        for p in new['processes']:
            self.assertEqual(p['env']['DPUMESH_SERVER'],f"DPUMesh{p['worker']}")
        self.assertEqual(new['worker_flow_budget'],old['worker_flow_budget'])
        self.assertTrue(all(p['env'].get('DPUMESH_BACKEND_MAX','16')=='16' for p in new['processes']))
        generate(16,spec,4,'0b:00.1','0-15',4,frontend_workers=[0]*4) # aliases do not pin flows
        with self.assertRaises(ValueError):generate(16,spec,4,'0b:00.1','0-15',4,frontend_workers=[16]*4)
        with self.assertRaises(ValueError):generate(16,spec,4,'0b:00.1','0-15',4,frontend_workers=[0])
    def test_staggered_backend_owners_and_budget(self):
        t=generate(4,{s:2 for s in PORTS},4,'0b:00.1','0-15',4,'staggered')
        self.assertEqual(t['worker_flow_budget'],[26]*4)
        owners={p['worker'] for p in t['processes'] if p['service'] in PORTS and p['replica']==1}
        self.assertEqual(owners,set(range(4)))
if __name__=='__main__': unittest.main()
