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
yenc_translation_table = [214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213]


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
                    
            data.append(yenc_translation_table[c])
            
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

    start_idx = 0
    
    # Read in an array of binary lines
    with open(name, "rb") as f:
        lines = list(f)
        
    # Skip any empty files
    if len(lines) == 0:
        return
    
    # Skip any lines superfluous lines
    while not lines[start_idx].startswith(b"=ybegin "):
        start_idx += 1
        
    # Extract header info
    header = keywords(lines[start_idx])
    multipart = "part" in header.keys()
    start_idx += 1 + multipart
        
    # We've found the beginning of the real data range
    end_idx = start_idx
    
    # Scan until we find the end of the real data range
    while not lines[end_idx].startswith(b"=yend "):
        end_idx += 1
        
    # Extract trailer, data, and CRC information
    trailer = keywords(lines[end_idx])
    data = decode(b"".join(lines[start_idx:end_idx]))     
    key = "pcrc32" if multipart else "crc32"
    
    # Do the CRC check, if needed
    if key in trailer.keys():
        crc1 = zlib.crc32(data) & 0xFFFFFFFF
        crc2 = int(trailer[key], 16)
        
        if not crc1 == crc2:
            log(f"CRC ERROR IN {name} ({crc1} != {crc2})") 
            return
        
    # Write or append data to the output file
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
            
            if os.path.exists(f['name']) and os.stat(f['name']).st_size == f['bytes']:
                log(f"File '{f['name']}' already exists. Skipping.")
                continue
        
            
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
    try:
        Slurp()
        rv = 0
    except KeyboardInterrupt:
        clean()
        rv = 32
        
    sys.exit(rv)
