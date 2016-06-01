"""Miscellaneous utilities for working with files and directories.
"""
import os
import sys
import pathlib
import io
import contextlib
import tempfile
import math
import hashlib


BUFFER_SIZE = 1048576


def makedir(directory, *, parents=False):
    "Make a directory if none exists."
    if hasattr(directory, '__fspath__'):
        directory = directory.__fspath__()
    directory = pathlib.Path(directory).absolute()
    try:
        directory.mkdir(parents=parents)
    except FileExistsError:
        if not directory.is_dir():
            raise
    return directory


def is_path(src):
    "Determine if the source is a path."
    return isinstance(src, (str, bytes, pathlib.Path)) or \
        hasattr(src, '__fspath__')


def open_file(file_name, mode='rt', *, encoding=None, errors=None,
              newline=None):
    "Open the file while accommodating multiple types of types."
    if not isinstance(file_name, (str, bytes)):
        if hasattr(file_name, '__fspath__'):
            file_name = file_name.__fspath__()
        else:
            file_name = str(file_name)
    if 't' not in mode and 'b' not in mode:
        mode += 't'
    return io.open(file_name, mode, encoding=encoding, errors=errors,
                   newline=newline)


def open_src_file(src, mode='rt', *, encoding=None, errors=None, newline=None):
    "Open the file if needed."
    managed = is_path(src)
    if managed:
        src = open_file(src, mode, encoding=encoding, errors=errors,
                        newline=newline)
    return src, managed


@contextlib.contextmanager
def open_src(src, mode='rt', *, encoding=None, errors=None, newline=None):
    "Conext manager to open a file if needed and close it on completion."
    fobj, managed = open_src_file(src, mode, encoding=encoding, errors=errors,
                                  newline=newline)
    try:
        yield fobj
    finally:
        if managed:
            fobj.close()


def line_count(src, mode='rt', **kwargs):
    """
    >>> line_count(io.StringIO('1\\n2\\n3\\n'))
    3
    """
    with open_src(src, mode, **kwargs) as fobj:
        return sum(1 for _ in fobj)


def _chardet(fobj, buffer_size):
    from chardet.universaldetector import UniversalDetector
    detector = UniversalDetector()
    detector_feed = detector.feed
    fobj_read = fobj.read
    data = fobj_read(buffer_size)
    while data:
        detector_feed(data)
        data = fobj_read(buffer_size)
    detector.close()
    return detector.result


def chardet(file_name, *, buffer_size=BUFFER_SIZE):
    """
    >>> a = tempfile.NamedTemporaryFile('xt', encoding='ascii', delete=False)
    >>> a.write('abc')
    3
    >>> a.close()
    >>> chardet(a.name)['encoding'] == 'ascii'
    True
    >>> os.unlink(a.name)

    >>> a = tempfile.NamedTemporaryFile('xt', delete=False)
    >>> a.write('abc\\u00d6')
    4
    >>> a.close()
    >>> chardet(a.name)['encoding'] == 'utf-8'
    True
    >>> os.unlink(a.name)

    >>> a = tempfile.NamedTemporaryFile('xt', encoding='cp1252', delete=False)
    >>> a.write('abc\\u00d6')
    4
    >>> a.close()
    >>> chardet(a.name)['encoding'] == 'ISO-8859-2'
    True
    >>> os.unlink(a.name)
    """
    with open_file(file_name, 'rb') as fobj:
        return _chardet(fobj, buffer_size)


def is_ascii(file_name, *, print_error=True, buffer_size=BUFFER_SIZE):
    """
    >>> a = tempfile.NamedTemporaryFile('xt', encoding='ascii', delete=False)
    >>> a.write('abc')
    3
    >>> a.close()
    >>> is_ascii(a.name, print_error=False)
    True
    >>> os.unlink(a.name)

    >>> a = tempfile.NamedTemporaryFile('xt', delete=False)
    >>> a.write('abc\\u00d6')
    4
    >>> a.close()
    >>> is_ascii(a.name, print_error=False)
    False
    >>> os.unlink(a.name)
    """
    with open_file(file_name, 'rb') as fobj:
        fobj_read = fobj.read
        data, seg = fobj_read(buffer_size), 0
        while data:
            for off, elem in enumerate(data):
                if (elem < 32 or elem > 126) and elem not in {10, 13}:
                    if print_error:
                        print('not ascii text', file=sys.stderr)
                        idx = seg * buffer_size + off
                        print(hex(elem), 'at', idx, '/', hex(idx),
                              file=sys.stderr)
                        fobj.seek(0)
                        result = _chardet(fobj, buffer_size)
                        print('possibly', result['encoding'], file=sys.stderr)
                    return False
            data, seg = fobj_read(buffer_size), seg + 1
    return True


# There are as many instances as neede
class ReadLines:        # pylint: disable=too-many-instance-attributes
    "Read lines from a file object delimited by str, byte, or regex."
    _closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        "Close the underlying objects."
        if not self._closed:
            self._closed = True
            self._fobj, fobj = None, self._fobj
            self._buf, self._idx, self._eof = '', 0, True
            if self._managed:
                fobj.close()

    def reset(self, delimiter=None):
        "Reset the stream to where it originally started."
        self._fobj.seek(self._start)
        self._buf = self._fobj.read(self.buffer_size)
        self._idx, self._eof = 0, not self._buf
        if delimiter is not None:
            self.delimiter = delimiter

    def peek(self, size=None):
        "Peek at the underlying buffer."
        if size is not None:
            if size <= 0:
                return ''
            buf, idx = self._buf, self._idx
            extra = size - (len(buf) - idx)
            if extra > 0 and not self._eof:
                fobj_read = self._fobj.read
                buffer_size = self.buffer_size
                to_read = math.ceil(extra / buffer_size) * buffer_size
                tmp_buf = fobj_read(to_read)
                tmp_buf_length = len(tmp_buf)
                if tmp_buf_length == 0:
                    self._eof = True
                buf += tmp_buf
                while 0 < tmp_buf_length < to_read:
                    to_read -= tmp_buf_length
                    tmp_buf = fobj_read(to_read)
                    tmp_buf_length = len(tmp_buf)
                    if tmp_buf_length == 0:
                        self._eof = True
                    buf += tmp_buf
                self._buf = buf
            return buf[idx:idx + size]
        buf, match_start, match_end = self._next()
        self._buf, self._idx = buf, match_start
        return buf[match_start:match_end]

    def __iter__(self):
        return self

    def __next__(self):
        buf, match_start, match_end = self._next()
        self._buf, self._idx = buf, match_end
        if match_end == 0:
            raise StopIteration
        return buf[match_start:match_end]

    def _next(self):
        delimiter = self.delimiter
        buf, idx = self._buf, self._idx
        search_idx = idx
        while True:
            if isinstance(delimiter, (str, bytes)):
                result = buf.find(delimiter, search_idx)
                if result != -1:
                    end = result + len(delimiter)
                    return buf, idx, end
                search_offset = len(delimiter) - 1
            else:
                # delimiter is not a string or byte here
                result = delimiter.search(buf, search_idx) # pylint: disable=no-member
                if result:
                    end = result.end()
                    if result.endpos != end:
                        return buf, idx, end
                    search_offset = end - result.start()
                else:
                    search_offset = len(buf) - idx
            if self._eof:
                end = len(buf)
                if idx < end:
                    return buf, idx, end
                return '', 0, 0
            buf, idx = buf[idx:], 0
            search_idx = len(buf) - search_offset
            if search_idx < 0:
                search_idx = 0
            more = self._fobj.read(self.buffer_size)
            buf += more
            if not more:
                self._eof = True

    def __init__(self, src, mode='rt', *, delimiter='\n', encoding=None,
                 errors=None, newline=None, buffer_size=BUFFER_SIZE):
        self._fobj, self._managed = open_src_file(src, mode, encoding=encoding,
                                                  errors=errors,
                                                  newline=newline)
        try:
            self._start = self._fobj.tell()
            self.buffer_size = buffer_size
            self._buf = self._fobj.read(self.buffer_size)
            self._idx, self._eof = 0, not self._buf
            self.delimiter = delimiter
        except:
            if self._managed:
                self._fobj.close()
            raise
        self._closed = False


def readlines(src, mode='rt', *, delimiter=None, encoding=None, errors=None,
              newline=None, buffer_size=BUFFER_SIZE):
    """
    >>> a = 'a~\\nb~\\nc~\\n'
    >>> l = list(readlines(io.StringIO(a)))
    >>> l == ['a~\\n', 'b~\\n', 'c~\\n']
    True

    >>> l = list(readlines(io.StringIO(a), delimiter='~'))
    >>> l == ['a~', '\\nb~', '\\nc~', '\\n']
    True
    """
    if delimiter is not None:
        with ReadLines(src, mode, delimiter=delimiter, encoding=encoding,
                       errors=errors, newline=newline,
                       buffer_size=buffer_size) as rdr:
            yield from rdr
    else:
        with open_src(src, mode, encoding=encoding, errors=errors,
                      newline=newline) as fobj:
            yield from fobj


def md5sum(file_name, *, buffer_size=BUFFER_SIZE):
    """
    >>> a = tempfile.NamedTemporaryFile('xt', delete=False)
    >>> a.write('abc')
    3
    >>> a.close()
    >>> md5sum(a.name)
    '900150983cd24fb0d6963f7d28e17f72'
    >>> os.unlink(a.name)
    """
    hsh = hashlib.md5()
    with open_file(file_name, 'rb') as fobj:
        fobj_read, hsh_update = fobj.read, hsh.update
        data = fobj_read(buffer_size)
        while data:
            hsh_update(data)
            data = fobj_read(buffer_size)
    return hsh.hexdigest()


def sha256sum(file_name, *, buffer_size=BUFFER_SIZE):
    """
    >>> a = tempfile.NamedTemporaryFile('xt', delete=False)
    >>> a.write('abc')
    3
    >>> a.close()
    >>> sha256sum(a.name)
    'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
    >>> os.unlink(a.name)
    """
    hsh = hashlib.sha256()
    with open_file(file_name, 'rb') as fobj:
        fobj_read, hsh_update = fobj.read, hsh.update
        data = fobj_read(buffer_size)
        while data:
            hsh_update(data)
            data = fobj_read(buffer_size)
    return hsh.hexdigest()
