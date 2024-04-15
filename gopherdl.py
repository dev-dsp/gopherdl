#!/usr/bin/env python3

# gopherdl.py

import getopt
import sys
import math
import urllib.parse
import time
import socket
import os
import re
import logging
import inspect

from typing import Callable

socket.setdefaulttimeout(5)


def log(f: Callable) -> Callable:
    def _str_slice(_str, maxlen=150):
        return (
            _str
            if len(_str) <= maxlen
            else _str[: maxlen // 2] + "..." + _str[-(maxlen // 2) :]
        )

    def _log(*args, **kwargs):
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)

        logging.debug(
            f"CALL {f.__name__}(args={_str_slice(str(args))}, kwargs={_str_slice(str(kwargs))}) CALLER {calframe[1][3]}()"
        )
        result = f(*args, **kwargs)
        logging.debug(
            f"EXIT {f.__name__}(args={_str_slice(str(args))}, kwargs={_str_slice(str(kwargs))}) RETURN {_str_slice(str(result))}"
        )
        return result

    return _log


# TOFIX: content=None on write_gopherurl is a bad codesmell, this function
# should only be called in one place


class Config:

    getopt_spec = "l:w:hrspcdmnO:A:R:M"

    def __init__(self, optdict):
        # Commandline options
        flags = optdict.keys()
        self.recursive = "-r" in flags
        self.maxdepth = math.inf if not "-l" in flags else int(optdict["-l"])
        self.spanhosts = "-s" in flags
        self.helpme = "-h" in flags
        self.clobber = "-c" in flags
        self.only_save_menu = "-m" in flags
        self.no_save_menu = "-n" in flags
        self.ascend_parents = "-p" in flags
        self.delay = 0.0 if not "-w" in flags else float(optdict["-w"])
        self.debug = "-d" in flags
        self.accept_regex = None if not "-A" in flags else re.compile(optdict["-A"])
        self.reject_regex = None if not "-R" in flags else re.compile(optdict["-R"])
        self.regex_on_menus = "-M" in flags
        self.archive_directory = str(optdict["-O"]) if "-O" in flags else "./archive/"

    def __str__(self):
        return f"""recursive = {self.recursive}
maxdepth = {self.maxdepth}
spanhosts = {self.spanhosts}
helpme = {self.helpme}
clobber = {self.clobber}
only_save_menu = {self.only_save_menu}
ascend_parents = {self.ascend_parents}
delay = {self.delay}
debug = {self.debug}
accept_regex = {self.accept_regex}
reject_regex = {self.reject_regex}
regex_on_menus = {self.regex_on_menus}
archive_directory = {self.archive_directory}
"""


def print_options():
    helpdoc = {
        "-r": "Enable recursive downloads",
        "-l [depth]": "Maximum depth in recursive downloads (default none)",
        "-s": "Span hosts on recursive downloads",
        "-h": "Show this help",
        "-c": "Enable file clobbering (overwrite existing)",
        "-m": "Only download gopher menus",
        "-n": "Never download gopher menus",
        "-p": "Allow ascension to the parent directories",
        "-w [seconds]": "Delay between downloads",
        "-d": "Enable debug messages",
        "-A": "Accept URL regex",
        "-R": "Reject URL regex",
        "-M": "Apply accept/reject regex rules to menus (can prevent recursion)",
        "-O": "Output directory for downloads (default ./archive/)",
    }

    for key, value in helpdoc.items():
        print("  {} {}".format(key, value))


class GopherURL:
    invalid_types = [
        "7",  # Search service
        "2",  # CSO
        "3",  # Error
        "8",
        "T",
    ]  # telnet

    def __init__(self, type, text, path, host, port):
        self.host = host
        self.port = port
        self.path = path
        self.text = text
        self.type = type

    def __str__(self):
        return (
            f"<GopherURL type={self.type} url={self.host}:{self.port} path={self.path}>"
        )

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            samehost = self.host == other.host
            samepath = self.path == other.path
            return samehost and samepath
        return False

    def __hash__(self):
        return hash(self.to_file_path())

    def valid(self):
        if len(self.path) == 0:
            return False
        if self.port <= 0:
            return False
        if self.type in GopherURL.invalid_types:
            return False
        if "URL:" in self.path:
            return False

        # If the path contains enough "../", it would be saved outside our
        # download directory, which is a security risk. Ignore these files
        file_path = os.path.relpath(self.to_file_path())
        in_download_dir = file_path.startswith(self.host)

        if not in_download_dir:
            return False

        return True

    def download(self, delay):
        retries = 0
        while True:
            time.sleep(delay)
            sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            buffer = bytearray()
            try:
                sock.connect((self.host, self.port))
                sock.send(bytes(self.path + "\r\n", "utf-8"))
                data = None
                while data != b"":
                    data = sock.recv(1024)
                    buffer.extend(data)
                sock.close()
            except (socket.gaierror, TimeoutError, ConnectionRefusedError) as e:
                retries += 1
                if retries > 3:
                    logging.warning(
                        f"{e} while downloading from {self.host}:{self.port}{self.path} after 3 retries"
                    )
                    break
                delay = delay + delay / 4 if delay != 0 else 1
                logging.debug(
                    f"{e} while downloading from {self.host}:{self.port}{self.path}, retrying ({retries}/3) in {delay}s"
                )
                continue
            return buffer

    def is_menu(self):
        return self.type == "1"

    # As it would look in a browser urlbar
    def to_url(self):
        path_parts = self.path.split("?")
        path, query = (
            (self.path, "")
            if len(path_parts) == 1
            else (path_parts[0], "?".join(path_parts[1:]))
        )
        return urllib.parse.urlunparse(("gopher", self.host, path, query, "", ""))

    # path without adding gophermap
    def to_url_path(self):
        path = self.path.strip("/").split("/")
        outfile = os.path.join(self.host, os.path.sep.join(path))
        return outfile

    def to_file_path(self):
        outfile = self.to_url_path()
        if self.is_menu():
            outfile = os.path.join(outfile, "gophermap")
        return outfile


def debug_list(lst, message, config):
    logging.debug(message)
    logging.log(5, lst)


def print_help_quit(ret):
    print("Usage: gopherdl.py [options] [url1 url2 ...]")
    print("Options:")
    print_options()
    quit(ret)


def mkdirs(path):
    cd = str()
    for p in path.split(os.path.sep):
        cd = os.path.join(cd, p)
        if not os.path.exists(cd):
            os.mkdir(cd)


def get_menus(gurls):
    return [g for g in gurls if g.is_menu()]


def get_files(gurls):
    return [g for g in gurls if not g.is_menu()]


def slurp(path):
    with open(path, "rb") as f:
        return f.read()


# Extract urls from a gopher menu
@log
def getlinks(menucontent, config):
    urls = []
    for line in menucontent.split(sep="\n"):
        tokens = line.strip().split(sep="\t")
        try:
            typ = tokens[0][0]
            if typ in ["i"]:
                logging.log(5, f"skipping informational {line}")
                continue
            text = tokens[0][1:]
            path = tokens[1].strip()
            host = tokens[2].strip()
            port = int(tokens[3].strip())

            url = GopherURL(typ, text, path, host, port)

            if not url.valid():
                logging.log(5, f"skipping invalid {url}")
                continue

            logging.debug(f"adding {url}")
            urls.append(url)

        except IndexError:
            logging.debug(f"Invalid line (IndexError): {', '.join(tokens)}")
        except ValueError as e:
            logging.debug("Invalid Port: {}".format(e))

    return urls


def write_gopherurl(gurl, config, content=None):
    outfile = os.path.join(config.archive_directory, gurl.to_file_path())

    # If it exists and config says no clobber, leave
    if os.path.exists(outfile) and not config.clobber:
        logging.debug("Not overwriting: %s", outfile)
        return

    mkdirs(os.path.dirname(outfile))
    content = content if content != None else gurl.download(config.delay)
    if content:
        logging.debug("write_gopherurl: {}".format(gurl))
        with open(outfile, "wb") as outfile:
            outfile.write(content)


# Return a tuple, (host,port,path)
def spliturl(urlstr):

    has_gopher_scheme = urlstr[0:9] == "gopher://"
    has_scheme = "://" in urlstr

    # They specified an incompatible protocol
    if has_scheme and not has_gopher_scheme:
        raise ValueError("Invalid scheme in url '{}'".format(urlstr))

    # Assume they meant gopher, give it an empty scheme
    if not has_scheme:
        urlstr = "//{}".format(urlstr)

    url = urllib.parse.urlsplit(urlstr)
    path = "/" if len(url.path) == 0 else url.path
    host = url.netloc.split(":")[0]
    port = 70 if url.port is None else url.port

    return (host, port, path + f"?{url.query}" if url.query else path)


def crawl(root_gurl, config):

    def gurl_ok_by_config(link):

        on_different_host = root_gurl.host != link.host
        if not config.spanhosts and on_different_host:
            logging.debug("Not spanning: {} != {}".format(root_gurl.host, link.host))
            return False

        off_original_path = not link.path.startswith(root_gurl.path)
        if not config.ascend_parents and off_original_path:
            logging.debug("Not Ascending: {} <-> {}".format(root_gurl.path, link.path))
            return False

        # If config says not to apply regex on menus, stop here if it is
        # If the link is a menu AND we don't apply regex on menus, return
        if not config.regex_on_menus and link.is_menu():
            return True

        # Filter by regular expressions
        url = link.to_url()

        if config.reject_regex != None:
            match = config.reject_regex.fullmatch(url)
            if match != None:
                logging.debug("Reject: {}".format(url))
                return False

        if config.accept_regex != None:
            match = config.accept_regex.fullmatch(url)
            if match != None:
                logging.debug("Accept: {}".format(url))
                return True
            else:
                return False

        return True

    def retrieve_menu_content(gurl):
        path = gurl.to_file_path()
        content = None
        if os.path.exists(path) and not config.clobber:
            logging.info("Using existing menu {}".format(path))
            content = slurp(path)
        else:
            try:
                content = gurl.download(config.delay)
            except (socket.gaierror, TimeoutError) as e:
                logging.warning(f"{e} while retrieving menu content from {gurl}")
                return None
        return content.decode("utf-8", errors="ignore") if content is not None else None

    @log
    def gopher_urls_from_menu_link(menu_gurl):
        menu_content = retrieve_menu_content(menu_gurl)
        if menu_content is None:
            return []

        gurls = getlinks(menu_content, config)
        debug_list(gurls, "Before filter # urls: {}".format(len(gurls)), config)

        gurls = list(filter(gurl_ok_by_config, gurls))
        debug_list(gurls, "After filter # urls: {}".format(len(gurls)), config)

        return gurls

    gurls = set(gopher_urls_from_menu_link(root_gurl))
    menus = list(set(get_menus(gurls))) + [root_gurl]
    logging.debug(f"initial menu items from {root_gurl}: {menus}")
    depth = 0

    for menu in menus:
        if depth > config.maxdepth:
            logging.debug("Maxdepth {} reached".format(config.maxdepth))
            break

        new_gurls = set(gopher_urls_from_menu_link(menu))
        new_unique_gurls = new_gurls.difference(gurls)
        gurls.update(new_unique_gurls)
        new_unique_gurls_menus = get_menus(new_unique_gurls)
        menus.extend(new_unique_gurls_menus)
        depth += 1

        logging.info(
            "{} | {}/{} | {} ".format(len(gurls), depth, len(menus), menu.to_url_path())
        )

    return gurls


def download_gopher_urls(gopher_urls, config):
    for i in range(len(gopher_urls)):
        gurl = gopher_urls[i]
        logging.info(
            "[{}/{}] {}".format((i + 1), len(gopher_urls), gurl.to_file_path())
        )
        write_gopherurl(gurl, config)


def gopherdl(host, config):

    # hueristic: probably a menu if there's no file extension or ends in /
    def probably_a_menu(path):
        end = path.split("/")[-1]
        return not "." in end or path[-1] == "/"

    host, port, path = spliturl(host)
    logging.info(f"host={host} port={port} path={path}")
    root_gurl_type = "1" if probably_a_menu(path) else "0"
    root_gurl = GopherURL(root_gurl_type, "[ROOT URL]", path, host, port)
    logging.debug("root_gurl: %s", root_gurl)

    if config.recursive:
        # Recursive download
        logging.info("Downloading menu tree")
        gopher_urls = crawl(root_gurl, config)
        gopher_files = get_files(gopher_urls)
        gopher_menus = get_menus(gopher_urls)

        if config.no_save_menu:
            gopher_menus = []

        if config.only_save_menu:
            gopher_files = []

        if gopher_urls == []:
            logging.info("Nothing to download")
            return

        if len(gopher_menus) > 0:
            logging.info("Downloading {} menus".format(len(gopher_menus)))
            download_gopher_urls(gopher_menus, config)

        if len(gopher_files) > 0:
            logging.info("Downloading {} files".format(len(gopher_files)))
            download_gopher_urls(gopher_files, config)

    else:
        # Single file download
        logging.info("Downloading single file %s", root_gurl.to_file_path())
        write_gopherurl(root_gurl, config)


def main():
    optlist, args = ([], [])
    try:
        optlist, args = getopt.getopt(sys.argv[1:], Config.getopt_spec)
    except getopt.GetoptError:
        print_help_quit(1)

    optdict = dict(optlist)
    config = Config(optdict)
    hosts = args

    logging.basicConfig(
        format="%(asctime)s - %(funcName)30s() - %(levelname)7s - %(message)s",
        level="DEBUG" if config.debug else "INFO",
    )
    logging.debug("config\n%s", config)
    logging.debug("hosts\n%s", hosts)

    if config.helpme:
        print_help_quit(0)
    elif hosts == []:
        print_help_quit(1)

    for host in hosts:
        try:
            gopherdl(host, config)
        except ValueError as e:
            logging.exception(e)


if __name__ == "__main__":
    main()
