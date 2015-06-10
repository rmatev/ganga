from __future__ import absolute_import

from .CredentialStore import CredentialStore

from .VomsProxy import VomsProxy

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from .exceptions import CredentialsError

from functools import wraps

def RequireCredential(function):
    """
    A decorator for accessing the credential store
    
    Uses the function's object's ``credential_requirements`` attribute
    
    It also sets the function's object's ``credential_filename`` attribute
    """
    @wraps(function)
    def wrapped_function(*args, **kwargs):
        from Ganga.GPI import credentialStore
        
        functions_class_object = args[0]
        
        try:
            proxy = credentialStore.get(functions_class_object.credential_requirements)
        except CredentialsError:
            raise CredentialsError('Cannot get proxy which matches requirements')
        
        if not proxy.isValid():
            raise CredentialsError('Proxy is invalid')
        
        #Set the object member attribute
        functions_class_object.credential_filename = proxy.location
        #TODO Or maybe something like:
        # function.__globals__['credential_filename'] = proxy.location
        # which will restrict the variable to being available inside the function's global scope which would enforce the strict usage of this decorator.
            
        return function(*args, **kwargs)
    return wrapped_function
