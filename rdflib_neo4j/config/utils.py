from rdflib_neo4j.config.const import NEO4J_AUTH_REQUIRED_FIELDS, WrongAuthenticationException


def check_auth_data(auth):
    """
    Checks if the required authentication fields are present.

    Parameters:
    - auth: A dictionary containing authentication data.

    Raises:
    - WrongAuthenticationException: If any of the required authentication fields is missing.
    """
    if auth is None:
        raise Exception(
            f"Please define the authentication dict. These are the required keys: {NEO4J_AUTH_REQUIRED_FIELDS}")
    for param_name in NEO4J_AUTH_REQUIRED_FIELDS:
        if param_name not in auth:
            raise WrongAuthenticationException(param_name=param_name)
        if not auth[param_name]:
            raise Exception(f"The key {param_name} is defined in the authentication dict but the value is empty.")
