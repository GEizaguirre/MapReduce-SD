import requests
import base64
import os

class CloudFunctions:

    def __init__(self, functions_config):
        """
        Constructor
        """
        self.api_key = str.encode(functions_config['secret_key'])
        self.endpoint = functions_config['endpoint'].replace('http:', 'https:')
        self.namespace = functions_config['namespace']

        auth = base64.encodebytes(self.api_key).replace(b'\n', b'')
        self.session = requests.session()
        default_user_agent = self.session.headers['User-Agent']
        self.headers = {
            'content-type': 'application/json',
            'Authorization': 'Basic %s' % auth.decode('UTF-8'),
            'User-Agent': default_user_agent + ' pywren-ibm-cloud'
        }

        self.session.headers.update(self.headers)
        adapter = requests.adapters.HTTPAdapter()
        self.session.mount('https://', adapter)

        msg = 'IBM Cloud Functions init for'
        print("{} Namespace: {}".format(msg, self.namespace))
        print("{} Host: {}".format(msg, self.endpoint))

    def create_action(self, action_name, code=None, kind='blackbox',
                      image='ibmfunctions/action-python-v3.6', is_binary=True, overwrite=True):
        """
        Create an IBM Cloud Function
        """
        print('I am about to create a new cloud function action')
        url = os.path.join(self.endpoint, 'api', 'v1', 'namespaces',
                           self.namespace, 'actions',
                           action_name + "?overwrite=" + str(overwrite)).replace('\\','/')

        data = {}
        limits = {}
        cfexec = {}

        limits['timeout'] = 600000
        limits['memory'] = 1024

        if limits['timeout'] and limits['memory']:
            data['limits'] = limits

        cfexec['kind'] = kind
        if kind == 'blackbox':
            cfexec['image'] = image
        cfexec['binary'] = is_binary
        cfexec['code'] = base64.b64encode(code).decode("utf-8") if is_binary else code
        data['exec'] = cfexec

        res = self.session.put(url, json=data)

        if res.status_code != 200:
            print('An error occurred updating action {}'.format(action_name))
        else:
            print("OK --> Updated action {}".format(action_name))

    def get_action(self, action_name):
        """
        Get an IBM Cloud Function
        """
        print("I am about to get a cloud function action: {}".format(action_name))
        url = os.path.join(self.endpoint, 'api', 'v1', 'namespaces',
                           self.namespace, 'actions', action_name).replace('\\','/')
        res = self.session.get(url)
        return res.json()

    def delete_action(self, action_name):
        """
        Delete an IBM Cloud Function
        """
        print("Delete cloud function action: {}".format(action_name))

        url = os.path.join(self.endpoint, 'api', 'v1', 'namespaces',
                           self.namespace, 'actions', action_name).replace('\\','/')
        res = self.session.delete(url)

        if res.status_code != 200:
            print('An error occurred deleting action {}'.format(action_name))

    def invoke(self, action_name, payload):
        """
        Invoke an IBM Cloud Function
        """
        url = os.path.join(self.endpoint, 'api', 'v1', 'namespaces',
                           self.namespace, 'actions', action_name).replace('\\','/')

        try:

            resp = self.session.post(url, json=payload)
            data = resp.json()
            resp_time = format(round(resp.elapsed.total_seconds(), 3), '.3f')
        except:
            return self.remote_invoke(action_name, payload)

        if 'activationId' in data:
            log_msg = ('Activation ID: {} - Time: {} seconds'.format(data["activationId"], resp_time))
            print(log_msg)
            return data["activationId"]
        else:
            print(data)
            return None

    def invoke_with_result(self, action_name, payload={}):
        """
        Invoke an IBM Cloud Function waiting for the result.
        """

        url = os.path.join(self.endpoint, 'api', 'v1',
                           'namespaces', self.namespace, 'actions',
                           action_name + "?blocking=true&result=true").replace('\\','/')
        print(url)
        resp = self.session.post(url, json=payload)
        result = resp.json()

        return result
