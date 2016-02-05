'''
Created on Jan 12, 2012

@author: eocampo
'''


infCode = {
        0  : 'Success ',
        1  : 'Integration Service is not available, or pmcmd cannot connect to the Integration Service. There is a problem with the TCP/IP host name or port number or with the network.',
        2  : 'Task name, workflow name, or folder name does not exist.',
        3  : 'An error occurred starting or running the workflow or task.',
        4  : 'Usage error. You passed the wrong options to pmcmd.',
        5  : 'An internal pmcmd error occurred. Contact Informatica Global Customer Support.',
        7  : 'You used an invalid user name or password.',
        8  : 'You do not have the appropriate permissions or privileges to perform this task.',
        9  : 'Connection to the Integration Service timed out while sending the request.',
        12 : 'Integration Service cannot start recovery because the session or workflow is scheduled, waiting for an event,waiting, initializing, aborting, stopping, disabled, or running.',
        13 : 'User name environment variable is set to an empty value.',
        14 : 'Password environment variable is set to an empty value.',
        15 : 'User name environment variable is missing.',
        16 : 'Password environment variable is missing.',
        17 : 'Parameter file does not exist.',
        18 : 'Integration Service found the parameter file, but it did not have the initial values for the session parameters, such as $input or $output.',
        19 : 'Integration Service cannot resume the session because the workflow is configured to run continuously.',
        20 : 'A repository error has occurred. Make sure that the Repository Service and the database are running and the number of connections to the database is not exceeded.',
        21 : 'Integration Service is shutting down and it is not accepting new requests.',
        22 : 'Integration Service cannot find a unique instance of the workflow/session you specified. Enter the command again with the folder name and workflow name.',
        23 : 'There is no data available for the request.',
        24 : 'Out of memory.',
        25 : 'Command is cancelled.',
}