"""
Copyright: Copyright (C) 2019 Beacon Platform Inc. - All Rights Reserved
Product: Core
Category: Core
Description: |
    External Python helper routines for accessing Beacon RPC from outside the environment.
    Intended as a basis for client development of their own external interfaces to Beacon.
    Please do not invoke these from within the Beacon enviroment: in particular, we advise
    against storing your personal tokens inside the shared Beacon environment where other
    users have access.

    As noted above, this is not intended to be executed in the Beacon IDE.

"""

import ast
import base64
import logging
import glob
import itertools
import json
import os
import time
import typing as t

import requests
import ssl
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# Reset SSL context to default
ssl._create_default_https_context = ssl.create_default_context

class RPC:
    HTTP_METHODS = {'get', 'post', 'put', 'delete'}
    USER_TOKEN_FILE_PATTERNS = ['beacon_token_*.json', 'beacon_user_token_*.json']
    CLIENT_ID_FILE_PATTERNS = ['beacon_client_id_*.json']

    def __init__(self, token_file_name=None, client_id_file_name=None, secrets_dir=None, api_url='/r/apps/wmp-proxy', command='exec', raise_for_status=True):
        """Helper class to present tokens and submit authenticated requests to Beacon.
        Requires a token file and a client ID file.  You can generate and download these files from the
        Manage Keys dialog within your Beacon Profile.
        The url invoked gets constructed as the base URL +  the api_url + "/" + the command ::

             https://YOURDOMAIN.wsq.io/r/apps/wmp-proxy/exec
             +--- base URL -----------+
                                       +-- api URL ---+
                                                        +-- command --+

        Note the token file specifies the base URL automatically.
        Keeping track of the api_url separately from the api makes it easier to invoke different commands on the same url

        Parameters
        ----------
            token_file_name : str, optional
                An explicitly named json file containing your token.  If blank, attempts to find automatically
            client_id_file_name : str
                An explicitly named json file containing your client id.  If blank, attempts to find automatically
            secrets_dir : str, optional
                If token/client id files are unespecified then we attempt to find the
                latest beacon_token_*.json token file and the first beacon_client_id_*.json client id
                file, in this named directory.
                Defaults to a folder named ``.beacon`` in your home directory
            api_url : str
                The default url to invoke, when calling member functions like `post` and `get`
            command : str
                The default endpoint to invoke on the url, when calling member functions like `post` and `get`
                (For example: if api_url is "r/apps/wmp-proxy" and command is "exec" then `post` will
                access the "r/apps/wmp-proxy/exec" endpoint)
            raise_for_status : bool
                if True, methods like `post` and `get` will raises Exception (from the requests API)
                when HTTP error statuses are encountered.  When False, those methods will just return the
                standard response from the requests API without raising exception.  Default is True.

        """

        self.secrets_dir = secrets_dir or os.path.expanduser('~/.beacon')
        self.api_url = api_url
        self.command = command
        self.raise_for_status = raise_for_status

        token_file_names = [token_file_name] if token_file_name else self._get_files_by_patterns(self.USER_TOKEN_FILE_PATTERNS)
        client_id_file_names = [client_id_file_name] if client_id_file_name else self._get_files_by_patterns(self.CLIENT_ID_FILE_PATTERNS)

        if not client_id_file_names:
            raise ValueError('No client id found')
        with open(client_id_file_names[0], 'r') as f:
            self.client_id = json.load(f)

        tokens = []
        for fn in token_file_names:
            try:
                with open(fn, 'r') as f:
                    tokens.append(json.load(f))
            except Exception:
                pass
        if not tokens:
            raise ValueError('No token files found')

        self.login_token = sorted(tokens, key=lambda tok: tok['created'])[0]
        self._auth_token = ''
        self._token_expiry = 0
        self.domain_url = self.login_token['url'].rstrip('/')
        self.auth_url = '/'.join((self.domain_url, 'login/authtoken'))
        logger.info('Will authenticate against: %s and invoke APIs against: %s', self.auth_url, self.api_url)

    def _get_files_by_patterns(self, patterns: t.List[str]) -> t.List[str]:
        return sorted(itertools.chain.from_iterable(map(lambda p: glob.glob(self.secrets_dir + '/' + p), patterns)))

    def get_or_renew_token(self) -> str:
        if time.time()+60 > self._token_expiry:
            # Need to reissue the token
            headers = {'Authorization': 'Token {},{},{},{}'.format(
                self.login_token['token_id'], self.login_token['token_secret'], self.client_id['client_id'], self.client_id['client_secret']
            )}
            logger.info('Requesting a new token from: %s', self.auth_url)
            r = requests.get(self.auth_url, headers=headers, verify=False)  # Disable SSL verification
            r.raise_for_status()
            self._auth_token = r.text
            # The _auth_token should be treated as a SECRET, do not expose it, do not log it etc etc.
            payload_data = self._auth_token.split('.', 2)[1]
            payload = json.loads(base64.urlsafe_b64decode(payload_data+'='*(4-len(payload_data)%4)).decode('utf-8'))
            self._token_expiry = payload['exp']
        return self._auth_token

    def _req(self, r, command, *args, **kws):
        """Issue a request adding the authentication cookie.
           Uses the domain_url/api_url/command
           or you can specify an alternative complete url using args[0]

           Parameters
           ----------
               r : str
                   should be 'post', 'get', 'put' or 'delete'
               command : str
                   Forms the last part of the url to access (domain_url/api_url/COMMAND)
                   Typically this will be something like 'exec' or 'rpc' etc
                   or 'download/<filename>' etc per the wmp_proxy documentations
               args : optional
                   Passed directly to the `requests` API
               kws : optional
                   Apart from `headers`, these kwargs are passed directly to the `requests` API.
                   We automatically add in the Beacon auth headers.

           Returns
           -------
               Result of requests API

           Raises
           ------
               Treats HTTP error status as exception, if raise_for_status is set
        """

        kws.setdefault('headers', {})['Authorization'] = 'Bearer ' + self.get_or_renew_token()

        url = self.api_url
        if not url.startswith(self.domain_url):
            url = '/'.join((self.domain_url, url.lstrip('/')))
        if command:
            url += '/' + command
        if r not in self.HTTP_METHODS:
            raise ValueError('Unsupported value for request method: %s', r)
        req = getattr(requests, r)
        logger.debug('API request: %s -> %s', r, url)
        req_result = req(url, *args, verify=False, **kws)  # Disable SSL verification
        if self.raise_for_status:
            req_result.raise_for_status()
        return req_result

    def post(self, *args, **kws):
        """Issues HTTPS POST to the default api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('post', self.command, *args, **kws)

    def get(self, *args, **kws):
        """Issues HTTPS GET to the default api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('get', self.command, *args, **kws)

    def put(self, *args, **kws):
        """Issues HTTPS PUT to the default api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('put', self.command, *args, **kws)

    def delete(self, *args, **kws):
        """Issues HTTPS DELETE to the default api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('delete', self.command, *args, **kws)

    def post_endpoint(self, endpoint, *args, **kws):
        """Issues HTTPS POST to the named api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('post', endpoint, *args, **kws)

    def get_endpoint(self, endpoint, *args, **kws):
        """Issues HTTPS GET to the named api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('get', endpoint, *args, **kws)

    def put_endpoint(self, endpoint, *args, **kws):
        """Issues HTTPS PUT to the named api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('put', endpoint, *args, **kws)

    def delete_endpoint(self, endpoint, *args, **kws):
        """Issues HTTPS DELETE to the named api endpoint (e.g. exec, publish, etc)
        Passes args and kws to the `requests` api
        """
        return self._req('delete', endpoint, *args, **kws)

    def post_exec(self, tasks=None, local_tasks=None, *, job_data=None):
        """Example of a wrapper around the 'exec' endpoint"""
        data = {}
        if tasks:
            data['tasks'] = tasks
        if local_tasks:
            data['local_tasks'] = local_tasks
        if job_data:
            data['job_data'] = job_data
        return self.post_endpoint('exec', data=json.dumps(data))

    def get_rpc(self, fn_name, **kws):
        """Example of a wrapper around the 'rpc' endpoint"""
        return self.get_endpoint('rpc/' + fn_name, params=kws)

    def post_rpc_json(self, fname, data=None):
        """Example of send json file through HTTP POST"""
        print('Calling: ', fname)
        return self.post_endpoint('rpc/' + fname, data=json.dumps(data), headers={'Content-Type': 'application/json'}, timeout=600)

    def get_bobreports(self, batch_date, job_name, report_name):
        """Example of a wrapper around the 'bob-reports' endpoint"""
        return self.get_endpoint('bob-reports/{}/{}/{}'.format(batch_date.strftime('%Y-%m-%d'), job_name, report_name))

    def get_download(self, filename):
        """Example of a wrapper around the 'download' endpoint"""
        return self.get_endpoint('download/' + filename)

    def post_upload(self, files, subdir, root=None):
        """Example of a wrapper around the 'upload' endpoint

        Files should be a dict of {destination_filename: file_object}
        """
        files_info = [('upload', (name, file)) for name, file in files.items()]
        data = {'subdir': subdir}
        if root:
            data['root'] = root
        return self.post_endpoint('upload', files=files_info, data=data)

    def post_publish(self, msgs, redis_name=None):
        """Example of a wrapper around the 'publish' endpoint"""
        data = {'msgs': msgs}
        if redis_name:
            data['redis_name'] = redis_name
        return self.post_endpoint('publish', data=json.dumps(data))


def main():
    # Create the helper class.  With no other args, this will automatically look in ~/.beacon
    # (or %HOME%/.beacon on Windows) for client ids and tokens, and will use the default domain
    # url based on the token it found
    rpc = RPC()

    # alternative, specify explicitly like:
    # rpc = RPC(client_id_file_name='beacon_client_wst.json', token_file_name='my_user_token.json')

    response = rpc.get_rpc('users/galaxy/jc/beacon_api/generate_gre')
    res_dict = ast.literal_eval(response.text)
    print(res_dict['content'])  # a list of lists, with each sublist being a row e.g. ['Pod(L2)', 'Strategy', 'Quantity'...]

if __name__ == '__main__':
    main()
