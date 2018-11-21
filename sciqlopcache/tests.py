import unittest

from pyramid import testing


class ViewTests(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()

    def tearDown(self):
        testing.tearDown()

    def test_my_view(self):
        from .views import my_view
        request = testing.DummyRequest()
        info = my_view(request)
        self.assertEqual(info['project'], 'sciqlopcache')


class FunctionalTests(unittest.TestCase):
    def setUp(self):
        from sciqlopcache import main
        app = main({})
        from webtest import TestApp
        self.testapp = TestApp(app)

    def test_root(self):
        res = self.testapp.get('/', status=200)
        self.assertTrue(b'Pyramid' in res.body)

    def test_auth(self):
        res = [self.testapp.get('/php/rest/auth.php', status=200).body for i in range(10)]
        self.assertTrue(res[1:]!=res[:-1])
