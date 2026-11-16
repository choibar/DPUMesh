"""Container startup must not weaken native registration authorization."""
import threading
import time
import unittest
from unittest.mock import Mock

from node.native_registration import wait_metadata
from node.workload_identity import IdentityError, IdentityNotReady


class RegistrationStartup(unittest.TestCase):
    def test_rechecks_after_kubelet_publishes_container(self):
        verifier = Mock()
        verifier.metadata.side_effect = [IdentityNotReady(), b'verified record']
        result = wait_metadata(verifier, 'worker', b'nonce', 'boot',
                               threading.Event(), time.monotonic() + 1)
        self.assertEqual(result, b'verified record')
        self.assertEqual(verifier.metadata.call_count, 2)

    def test_wrong_identity_is_not_retried(self):
        verifier = Mock()
        verifier.metadata.side_effect = IdentityError('wrong container ID')
        with self.assertRaises(IdentityError):
            wait_metadata(verifier, 'worker', b'nonce', 'boot',
                          threading.Event(), time.monotonic() + 1)
        self.assertEqual(verifier.metadata.call_count, 1)

    def test_startup_timeout_never_issues_record(self):
        verifier = Mock()
        verifier.metadata.side_effect = IdentityNotReady()
        with self.assertRaises(IdentityNotReady):
            wait_metadata(verifier, 'worker', b'nonce', 'boot',
                          threading.Event(), time.monotonic())


if __name__ == '__main__':
    unittest.main()
