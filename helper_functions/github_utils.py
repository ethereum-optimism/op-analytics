import pandas as pd
import requests
import re

def get_file_url_from_github(owner_name, repo_name, path_name, file_name):
        # Check path variable
        if not path_name.endswith('/'):
                path_name += '/'
        #fill github API
        gh_api = 'https://api.github.com/repos/OWNER/REPO/contents/PATH'
        # Code from StackOverflow - can't find the link now
        rep = {'OWNER': owner_name, 'REPO': repo_name, 'PATH':path_name + file_name} # define desired replacements here
        ## use these three lines to do the replacement
        rep = dict((re.escape(k), v) for k, v in rep.items()) 
        ## Python 3 renamed dict.iteritems to dict.items so use rep.items() for latest versions
        rep_pattern = re.compile("|".join(rep.keys()))

        req_api = rep_pattern.sub(lambda m: rep[re.escape(m.group(0))], gh_api)

        inf = requests.get(req_api)

        dl_url = inf.json()['download_url']

        return dl_url
        # r = requests.get(dl_url)
        
        # return r.text
        # f = open(file_name, 'wb')
        # for chunk in r.iter_content(chunk_size=512 * 1024): 
        #         if chunk:
        #                 f.write(chunk)
        # f.close()