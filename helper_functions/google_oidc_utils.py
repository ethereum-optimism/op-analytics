import logging
import requests
import subprocess
import shlex

class GCPLogin:
    def __init__(self):
        self.access_token=None
        try:
            self.current_user_email=self.retrieve_current_user_email()
            self.project_id=self.current_user_email.split("@")[1].split(".")[0]
        except:
            logging.critical("No user currently logged in. Make sure you have an active user in gcloud")    
            raise Exception("No user found logged in")

    def get_project_id(self):
        return self.project_id
    
    def get_current_user_email(self):
        return self.current_user_email
    
    def get_access_token(self,force_refresh:bool=False):
        if force_refresh or not self.access_token:
            self.access_token=self.retrieve_access_token()
        return self.access_token
        
    def __str__(self):
        return f"GCPLogin(User: {self.current_user_email})"
    
    def retrieve_current_user_email(self):
        url = "https://www.googleapis.com/oauth2/v1/userinfo?alt=json"
        response = self.send_request(url=url, method="GET", headers=None, data=None)
        if response['data'] and  "email" in response['data']:
            return response['data']['email']
        return None
    
    def retrieve_access_token(self):
        cmd = "gcloud auth print-access-token"
        stdout, stderr = self.execute_shell_command(cmd,timeout=2)
        if stderr is None:
            return stdout.strip()
        else:
            raise Exception("Failed to print access token. Please ensure you are properly authenticated and try again.")
    
    def send_request(self,url:str, method:str="GET", headers:dict=None, data:dict=None):
        """
        Send an HTTP request and return the response.

        :param url: URL to send the request to
        :param method: HTTP method (GET or POST)
        :param headers: Dictionary of headers to send with the request
        :param data: Data to send with the request. For GET requests, these will be converted to URL parameters; for POST requests, this will be the request body.
        :return: A dictionary with the status code, response data, and any error message.
        """
        try:
            if not url.startswith("https://"):
                raise ValueError("URL must start with https:// for security reasons")
            # Ensure headers and data are not None
            if headers is None:
                headers = {}
            if data is None:
                data = {}
            headers['Content-Type']="application/json"
            headers['Authorization']=f"Bearer {self.get_access_token()}"
            # Choose the request method
            if method.upper() == "GET":
                response = requests.get(url, headers=headers)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, data=data)
            else:
                return {"error": "Unsupported method specified"}

            # Check if the response was successful
            response.raise_for_status()

            # Return the response status code and content
            try:
                response_data = response.json()
            except ValueError:
                response_data = response.text
            return {
                "status_code": response.status_code,
                "data": response_data,  # or response.text if expecting text
                "error": None
            }
        except requests.RequestException as e:
            # Handle any errors that occur during the request
            return {
                "status_code": None,
                "data": None,
                "error": str(e)
            }
        
    def execute_shell_command(self,cmd:str,timeout:int=5):
        """
        Executes a shell command and returns the output.

        Parameters:
        - cmd (str): The command to execute.

        Returns:
        - A tuple containing the command's standard output and standard error.
        """
        if not isinstance(cmd, str) or ';' in cmd or '&&' in cmd or '||' in cmd:
            raise ValueError("Invalid command. Command must be a safe string.")
        try:
            # Use shlex.split to handle command parsing.
            process = subprocess.run(shlex.split(cmd), check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,timeout=timeout)
            stdout = process.stdout
            return stdout, None  # Return stdout and None for stderr in case of success.
        except subprocess.CalledProcessError as e:
            return e.stdout, e.stderr  # Return both stdout and stderr in case of error.
        except subprocess.TimeoutExpired as e:
            return e.stdout, e.stderr
