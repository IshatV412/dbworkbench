"""Dynamic per-user database connection management.

Connections are made to the user's external PostgreSQL database using
credentials stored (Fernet-encrypted) in their ConnectionProfile.
Django ORM never touches the user's database — only this module does.
"""

import psycopg2


def get_user_connection(connection_profile):
    """Open a psycopg2 connection to the user's external database.

    Parameters
    ----------
    connection_profile : connections.models.ConnectionProfile
        Django model instance with encrypted credentials.

    Returns
    -------
    psycopg2 connection — caller is responsible for closing it.
    """
    return psycopg2.connect(
        host=connection_profile.host,
        port=connection_profile.port,
        dbname=connection_profile.database_name,
        user=connection_profile.db_username,
        password=connection_profile.get_decrypted_password(),
    )
