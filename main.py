from __future__ import annotations

from collections import defaultdict
import os
from pathlib import Path
from typing import Optional
import psycopg
import datetime
import re

import psycopg.connection

if os.name == "nt":
    import win32security

    def get_file_owner(file_path:os.DirEntry[str]):
        try:
            security_descriptor = win32security.GetFileSecurity(
                file_path.path, win32security.OWNER_SECURITY_INFORMATION
            )
            owner_sid = security_descriptor.GetSecurityDescriptorOwner()
            owner_name,_,_ = win32security.LookupAccountSid("", owner_sid)
        except:
            owner_name = "unknown"
        return owner_name

else:
    import pwd

    def get_file_owner(file_path):
        return pwd.getpwuid(file_path.stat().st_uid).pw_name


class DirNode:
    def __init__(self, path: Path, parent: Optional[DirNode] = None):
        self.path = path
        self.name = path.name
        self.parent: Optional[DirNode] = parent
        self.children: dict[Path, Optional[DirNode]] = {}
        self.generator = os.scandir(self.path)
        self.current: Optional[DirNode] = None
        self.done:bool = False

    def clear(self):
        del self.generator
        self.children.clear()

    def get_child(self, name: Path) -> Optional[DirNode]:
        if name in self.children:
            return self.children[name]
        else:
            res = None
            try:
                if not reg.match(str(name)):
                    res = DirNode(name, self)
            except:
                pass
            self.children[name] = res
            return res
    
    def add_forbidden_child(self,name:Path):
        self.children[name]=None

    def next(self) -> Optional[os.DirEntry[str]]:
        if self.done:
            return None

        if self.current:
            if entry := self.current.next():
                return entry
            self.current.clear()
            self.current = None

        try:
            entry = next(self.generator)
        except StopIteration:
            self.done = True
            return None

        if entry.is_dir():
            self.current = self.get_child(Path(entry.path))
            return self.next()

        return entry

    def serialize(self, path: Path) -> list[str]:
        path = path / self.name
        if len(self.children) > 0:
            res:list[str] = []
            for p,child in self.children.items():
                if child:
                    res.extend(child.serialize(path))
                else:
                    res.append(str(p))
            return res
        if self.done:
            return [str(path)]
        return []

    def add_forbidden(self, parts: list[str], path: Path):
        path = path / parts.pop(0)
       
        if len(parts) == 0:
            self.add_forbidden_child(path)
        else:
            c = self.get_child(path)
            if c:
                c.add_forbidden(parts, path)

class RootNode():
    def __init__(self, paths:list[Path]) -> None:
        self.children = [DirNode(path) for path in paths]
        self.paths = paths
        self.counter = 0

    def next(self) -> Optional[os.DirEntry[str]]:
        if len(self.children)<=self.counter:
            return None
        if entry := self.children[self.counter].next():
            return entry
        self.counter += 1
        return self.next()

    def serialize(self):
        res:list[str] = []
        for c in range(min(self.counter+1,len(self.children))):
            res.extend(self.children[c].serialize(self.paths[c].parent))
        return res

    def add_forbidden(self, path:Path):
        for c,child in enumerate(self.children):
            try:
                rel = path.relative_to(child.path)
            except ValueError:
                continue
            if rel == Path():
                raise ValueError("Cannot exclude a target directory")
            child.add_forbidden(list(rel.parts), self.paths[c])
            break

class Fringe:
    def __init__(self, paths: list[str]):
        self.root = RootNode([Path(path) for path in paths])

    def save(self, path: Path):
        with path.open("w") as f:
            f.writelines(
                [line + "\n" for line in self.root.serialize()]
            )

    def exclude_paths(self, path: Path):
        with path.open("r") as f:
            lines = f.read().splitlines()
        for line in lines:
            self.root.add_forbidden(Path(line))

Entry = tuple[str,str,str,str,int,str,str,str]
def insert_values(entries:list[Entry]):
    db:psycopg.connection.Connection = psycopg.connect("dbname=postgres user=postgres password=assword host=localhost port=5432")
    cur = db.cursor()
    cur.executemany('''
    insert into files (path, name, type, owner, size, modification, creation, access) values (%s, %s, %s, %s, %s, %s, %s, %s) on conflict do nothing;
    ''', entries)
    db.commit()
    db.close()
    entries.clear()

def init_table(path:Path):
    db =psycopg.connect("dbname=postgres user=postgres password=assword host=localhost port=5432")
    db.execute(path.read_bytes())
    db.commit()
    db.close()

def main(fringe:Fringe):
    entries:list[Entry] = []
    root = fringe.root
    while n := root.next():
        p = Path(n.path)
        name = p.stem
        path = str(p.parent)
        suffix = p.suffix
        owner = get_file_owner(n)
        stat = n.stat()
        size = stat.st_size
        mtime = datetime.date.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d")
        ctime = datetime.date.fromtimestamp(stat.st_birthtime).strftime("%Y-%m-%d")
        atime = datetime.date.fromtimestamp(stat.st_atime).strftime("%Y-%m-%d")

        entry = (path, name, suffix, owner, size, mtime, ctime, atime)
        entries.append(entry)
        if len(entries)>CHUNK_SIZE:
            insert_values(entries)
            fringe.save(Path(HISTORY))
    insert_values(entries)
    fringe.save(Path(HISTORY))

def check_targets(targets:list[str])->bool:
    paths = [Path(path) for path in targets]
    map:dict[Path,int] = defaultdict(int)
    for path in paths:
        map[path]+=1
        for p in path.parents:
            map[p]+=1
    for path in paths:
        if map[path]>1:
            return False
    return True


TARGETS = ["//srvnas/Documenti"] # Devono essere cartelle disgiunte!
HISTORY = "history.txt"
EXCLUSION = [HISTORY,"exclude.txt"]
REGEXES = "regex.txt"
INIT = "init.sql"
CHUNK_SIZE = 100

if __name__=="__main__":
    regex = "(?:"+")|(?:".join((line for line in Path(REGEXES).read_text().splitlines() if len(line)>0)) + ")"
    reg = re.compile(regex)

    if not check_targets(TARGETS):
        print("Targets can't be subdirectories of each other!")
        exit(1)
    fringe = Fringe(TARGETS)
    for file in EXCLUSION:
        fringe.exclude_paths(Path(file))
    init_table(Path(INIT))
    main(fringe)
