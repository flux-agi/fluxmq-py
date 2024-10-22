import logging

def with_host(host):
    def connection_opt(c):
        c.host = host
    return connection_opt

def with_logger(logger):
    def connection_opt(c):
        c.logger = logger.getChild("fluxmq")
    return connection_opt