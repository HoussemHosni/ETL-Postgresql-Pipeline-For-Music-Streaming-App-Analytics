from configparser import ConfigParser


def get_db(filename, section):
    """
    Description: This function is responsible for returning the database studentdb
    after parsing the config file database.ini
    
    Keyword arguments:
        filename      -- the configuration file of the database studentdb
        section     -- The section in the config file which is Postgresql
    Returns:
        db -- a dictionary of the studentdb
    """
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} does not exist in the config file {filename}")
    
    return db