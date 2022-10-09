#!/usr/bin/env python3
import nntplib
import os
import re
import time
import sys
import xml.etree.ElementTree as ET
import zlib

from fnmatch import fnmatchcase
from queue import Queue
from threading import Thread

#from numba import jit, byte
from humanfriendly import format_size
from mlargparser.mlargparser import MLArgParser

watchdog = Queue()


def clean():
    while not watchdog.empty():
        try:
            os.remove(watchdog.get())
        except OSError:
            pass


def die(s):
    log(s)
    sys.exit(1)


def loadnzb(s):
    ns = "{http://www.newzbin.com/DTD/2003/nzb}"
    pat = re.compile(r'"(.*)"')
    nzb = []
    tree = ET.parse(s)
    root = tree.getroot()
    
    for e in root.iter(ns + "file"):
        name = pat.search(e.attrib["subject"]).group(1)
        date = time.localtime(int(e.attrib["date"]))
        groups = []
        
        for g in e.iter(ns + "group"):
            groups.append(g.text)
            
        segments = []
        total = 0
        
        for s in e.iter(ns + "segment"):
            size = int(s.attrib["bytes"])
            total += size
            number = int(s.attrib["number"])
            sname = s.text
            segments.append({"bytes": size, "number": number, "name": sname})
            
        nzb.append({
            "name": name,
            "date": date,
            "groups": groups,
            "segments": sorted(segments, key=lambda x: x["number"]),
            "bytes": total,
        })
        
    return sorted(nzb, key=lambda x: x["name"])


def log(s, end="\n"):
    sys.stderr.write(s + end)
    sys.stderr.flush()
    

def ydec(name):
    #@jit(byte[:](byte[:]))
    def decode(buf):
        data = bytearray()
        esc = False
        
        for c in buf:
            if c == 13 or c == 10:
                continue
            
            if c == 61 and not esc:
                esc = True
                continue
            else:
                if esc:
                    esc = False
                    c -= 64
                    
                if 0 <= c <= 41:
                    dec = c + 214
                else:
                    dec = c - 42
                    
            data.append(dec)
            
        return data

    def keywords(line):
        words = line.decode("utf-8").split("=")
        k = words[1].split()[1]
        d = {}
        
        for s in words[2:-1]:
            pair = s.split()
            d[k] = pair[0]
            k = pair[1]
            
        d[k] = words[-1].strip()
        return d

    i = 0
    
    with open(name, "rb") as f:
        lines = list(f)
        
    if len(lines) == 0:
        return
    
    while not lines[i].startswith(b"=ybegin "):
        i += 1
        
    header = keywords(lines[i])
    i += 1
    multipart = "part" in header.keys()
    
    if multipart:
        i += 1
        
    j = i
    
    while not lines[j].startswith(b"=yend "):
        j += 1
        
    trailer = keywords(lines[j])
    data = decode(b"".join(lines[i:j]))
    key = "pcrc32" if multipart else "crc32"
    
    if key in trailer.keys():
        crc1 = zlib.crc32(data) & 0xFFFFFFFF
        crc2 = int(trailer[key], 16)
        
        if not crc1 == crc2:
            return
        
    mode = "ab" if multipart and int(header["part"]) != 1 else "wb"
    
    with open(header["name"], mode) as f:
        f.write(data)

class Slurp (MLArgParser):
    """ An NZB downloader """
  
    argDesc = {
        'filename': 'The full or relative path to the NZB file to process',
        'servername': 'The hostname of an NNTP server from which to download articles',
        'username': 'A username to use when authenticating to the NNTP server',
        'password': 'A password to use when authenticating to the NNTP server',
        'num_threads': 'The number of download threads to spawn when downloading articles',
        'tls': 'A flag to indicate whether or not to encrypt connections to the NNTP server',
    }
    
    def __fetch__(self, segment, groups):
        conn = nntplib.NNTP_SSL if self.use_tls else nntplib.NNTP
        port = 563 if self.use_tls else 119
        host = self.nntp_host
        
        if ":" in host:
            host, port = host.split(":", 1)
            port = int(port)
        try:
            s = conn(host, port)
            s.login(self.nntp_user, self.nntp_pass)
            
            for group in groups:
                try:
                    s.group(group)
                    watchdog.put(segment)
                    s.body("<{}>".format(segment), file=segment)
                except nntplib.NNTPTemporaryError:
                    continue
                break
            
            s.quit()
        except nntplib.NNTPPermanentError as e:
            log("slurp: nntp: " + str(e))

    def list(self, filename: str):
        """ List the files to be downloaded for a given NZB file """

        files = loadnzb(filename)
        total = 0
        ts = time.time()
        
        for f in files:
            size = f["bytes"]
            total += size
            fmt = "%b %e %H:%M" if ts - time.mktime(f["date"]) < 1.577e7 else "%b %e %Y"
            date = time.strftime(fmt, f["date"])
            print("{} {:>12} {}".format(format_size(size), date, f["name"]))
            
        if len(files) > 1:
            print("total " + format_size(total))
            
        return 0
    
    def download(self, filename: str, servername: str, username: str = "", password: str = "", num_threads: int = 10, tls: bool = False):
        self.nntp_host = servername
        self.nntp_user = username
        self.nntp_pass = password
        self.use_tls = tls
        files = loadnzb(filename)
        
        for f in files:
            j = 0
            n = len(f["segments"])
            total = f["bytes"]
            pend = 0
            
            while n > 0:
                threads = []
                
                for i in range(min(n, num_threads)):
                    t = Thread(target=self.__fetch__, args=(f["segments"][j]["name"], f["groups"]))
                    t.start()
                    threads.append(t)
                    pend += f["segments"][j]["bytes"]
                    j += 1
                    
                for t in threads:
                    t.join()
                    
                log(f"\r{f['name']}: {100.0*pend/total:3.0f} %", end="")
                n -= num_threads
                
            if watchdog.empty():
                return 1
            
            for segment in f["segments"]:
                ydec(segment["name"])
                
            clean()
            
        return 0

if __name__ == "__main__":
    #if parse() or len(sys.argv) == 0:
    #    usage()
    try:
        Slurp()
        rv = 0
    except KeyboardInterrupt:
        clean()
        rv = 32
        
    sys.exit(rv)
