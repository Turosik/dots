import getopt
import os
import sys
import configparser


def get_config() -> dict:
    cli_parameters = get_parameters()
    config_file = cli_parameters.get('-c', None) or cli_parameters.get('--config', None)
    if not config_file:
        print_help()
        sys.exit(2)
    print('Got config file {}'.format(config_file))
    return get_config_from_file(config_file)


def get_config_from_file(path) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(path)
    return config


def print_help():
    print('usage: ./run.py --config=</path/to/config/file>')


def get_parameters() -> dict:
    # get options from command line
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hc:', ['config='])
    # exit if given options are not specified
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    # exit if there are no options
    if (len(opts)) < 1:
        print_help()
        sys.exit(0)

    # process correct options
    cli_parameters = {}
    for opt, arg in opts:
        # output help for help
        if opt == '-h':
            print_help()
            sys.exit(0)
        # return argument if correct option -f is specified and given as argument file exists
        elif opt in ('-c', '--config'):
            if os.path.exists(arg) and os.path.isfile(arg):
                cli_parameters.update({opt: arg})
            else:
                print('file not found')
                sys.exit(0)
        else:
            cli_parameters.update({opt: arg})

    return cli_parameters


CONFIG_FILE = get_config()
