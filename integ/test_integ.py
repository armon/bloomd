import os
import os.path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import random

try:
    import pytest
except ImportError:
    print >> sys.stderr, "Integ tests require pytests!"
    sys.exit(1)


def pytest_funcarg__servers(request):
    "Returns a new APIHandler with a filter manager"
    # Create tmpdir and delete after
    tmpdir = tempfile.mkdtemp()
    port = random.randint(2000, 60000)

    # Write the configuration
    config_path = os.path.join(tmpdir, "config.cfg")
    conf = """[bloomd]
data_dir = %(dir)s
port = %(port)d
""" % {"dir": tmpdir, "port": port}
    open(config_path, "w").write(conf)

    # Start the process
    proc = subprocess.Popen("./bloomd -f %s" % config_path, shell=True)
    proc.poll()
    assert proc.returncode is None

    # Define a cleanup handler
    def cleanup():
        try:
            subprocess.Popen("kill -9 %s" % proc.pid, shell=True)
            time.sleep(1)
            shutil.rmtree(tmpdir)
        except:
            pass
    request.addfinalizer(cleanup)

    # Make a connection to the server
    connected = False
    for x in xrange(3):
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn.settimeout(1)
            conn.connect(("localhost", port))
            connected = True
            break
        except Exception, e:
            print e
            time.sleep(1)

    # Die now
    if not connected:
        raise EnvironmentError("Failed to connect!")

    # Make a second connection
    conn2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn2.settimeout(1)
    conn2.connect(("localhost", port))

    # Return the connection
    return conn, conn2


class TestInteg(object):
    def test_list_empty(self, servers):
        "Tests doing a list on a fresh server"
        server, _ = servers
        fh = server.makefile()
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

    def test_list_prefix(self, servers):
        "Tests lists with prefix"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar2\n")
        server.sendall("create foobar1\n")
        server.sendall("create test4\n")
        assert fh.readline() == "Done\n"
        assert fh.readline() == "Done\n"
        assert fh.readline() == "Done\n"
        time.sleep(1) # Wait for vacuum
        server.sendall("list foo\n")
        assert fh.readline() == "START\n"
        assert "foobar1" in fh.readline()
        assert "foobar2" in fh.readline()
        assert fh.readline() == "END\n"

    def test_create(self, servers):
        "Tests creating a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"

    def test_create_bad(self, servers):
        "Tests creating a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create " + ("foo"*100) + "\n")
        assert fh.readline() == "Client Error: Bad filter name\n"

    def test_doublecreate(self, servers):
        "Tests creating a filter twice"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("create foobar\n")
        assert fh.readline() == "Exists\n"

    def test_drop(self, servers):
        "Tests dropping a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"
        server.sendall("drop foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

    def test_close(self, servers):
        "Tests closing a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"
        server.sendall("close foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"

    def test_clear(self, servers):
        "Tests clearing a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create cleartest\n")
        assert fh.readline() == "Done\n"

        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "cleartest" in fh.readline()
        assert fh.readline() == "END\n"

        server.sendall("clear cleartest\n")
        assert fh.readline() == "Filter is not proxied. Close it first.\n"

        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "cleartest" in fh.readline()
        assert fh.readline() == "END\n"

        server.sendall("close cleartest\n")
        assert fh.readline() == "Done\n"

        server.sendall("clear cleartest\n")
        assert fh.readline() == "Done\n"

        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

        # Load + Drop the filter
        time.sleep(3)
        server.sendall("create cleartest\n")
        assert fh.readline() == "Done\n"
        server.sendall("drop cleartest\n")
        assert fh.readline() == "Done\n"

    def test_set(self, servers):
        "Tests setting a value"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_bulk(self, servers):
        "Tests setting bulk values"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("multi foobar test blah\n")
        assert fh.readline() == "No No\n"
        server.sendall("bulk foobar test blah\n")
        assert fh.readline() == "Yes Yes\n"

    def test_doubleset(self, servers):
        "Tests setting a value"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "No\n"

    def test_check(self, servers):
        "Tests checking a value"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("check foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_multi(self, servers):
        "Tests checking multiple values"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("multi foobar test test1 test2\n")
        assert fh.readline() == "No No No\n"
        server.sendall("set foobar test\n")
        assert fh.readline() == "Yes\n"
        server.sendall("multi foobar test test1 test2\n")
        assert fh.readline() == "Yes No No\n"

    def test_aliases(self, servers):
        "Tests aliases"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("b foobar test test1 test2\n")
        assert fh.readline() == "Yes Yes Yes\n"
        server.sendall("s foobar test\n")
        assert fh.readline() == "No\n"
        server.sendall("m foobar test1 test2\n")
        assert fh.readline() == "Yes Yes\n"
        server.sendall("c foobar test\n")
        assert fh.readline() == "Yes\n"

    def test_set_check(self, servers):
        "Tests setting and checking many values"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar\n")
        assert fh.readline() == "Done\n"
        for x in xrange(1000):
            server.sendall("set foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"
        for x in xrange(1000):
            server.sendall("check foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"

    def test_concurrent_drop(self, servers):
        "Tests setting values and do a concurrent drop on the DB"
        server, server2 = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(10000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                if resp != "Yes\n":
                    assert resp == "Filter does not exist\n" and x > 100
                    return
                else:
                    assert resp == "Yes\n"
            assert False

        def drop():
            time.sleep(0.2)
            server2.sendall("drop pingpong\n")

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=drop)
        t.start()
        loopset()

    def test_concurrent_close(self, servers):
        "Tests setting values and do a concurrent close on the DB"
        server, server2 = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(100000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                assert resp == "Yes\n"

        def close():
            time.sleep(0.1)
            server2.sendall("close pingpong\n")

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=close)
        t.start()
        loopset()

    def test_concurrent_flush(self, servers):
        "Tests setting values and do a concurrent flush"
        server, server2 = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(10000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                assert resp == "Yes\n"

        def flush():
            for x in xrange(3):
                time.sleep(0.1)
                server2.sendall("flush pingpong\n")

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=flush)
        t.start()
        loopset()

    def test_concurrent_create(self, servers):
        "Tests creating a filter with concurrent sets"
        server, server2 = servers
        fh = server.makefile()

        def loopset():
            for x in xrange(1000):
                server.sendall("set pingpong test%d\n" % x)
                resp = fh.readline()
                assert resp == "Yes\n"
            for r in xrange(3):
                for x in xrange(1000):
                    server.sendall("set pingpong%d test%d\n" % (r, x))
                    resp = fh.readline()
                    assert resp == "Yes\n"

        def create():
            for x in xrange(10):
                server2.sendall("create pingpong%d\n" % x)

        server.sendall("create pingpong\n")
        fh.readline() == "Done\n"
        t = threading.Thread(target=create)
        t.start()
        loopset()

    def test_create_in_memory(self, servers):
        "Tests creating a filter in_memory, tries flush"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar in_memory\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert "foobar" in fh.readline()
        assert fh.readline() == "END\n"
        server.sendall("flush foobar\n")
        assert fh.readline() == "Done\n"

    def test_set_check_in_memory(self, servers):
        "Tests setting and checking many values"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar in_memory\n")
        assert fh.readline() == "Done\n"
        for x in xrange(1000):
            server.sendall("set foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"
        for x in xrange(1000):
            server.sendall("check foobar test%d\n" % x)
            assert fh.readline() == "Yes\n"

    def test_drop_in_memory(self, servers):
        "Tests dropping a filter"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create foobar in_memory\n")
        assert fh.readline() == "Done\n"
        server.sendall("drop foobar\n")
        assert fh.readline() == "Done\n"
        server.sendall("list\n")
        assert fh.readline() == "START\n"
        assert fh.readline() == "END\n"

    def test_in_progress_drop(self, servers):
        "Tests creating/dropping a filter and getting the 'Delete in progress'"
        server, _ = servers
        fh = server.makefile()

        for x in xrange(10):
            # Create and drop should cause the vacuum to fall behind
            server.sendall("create drop_in_prog\n")
            assert fh.readline() == "Done\n"
            server.sendall("drop drop_in_prog\n")
            assert fh.readline() == "Done\n"

            # Create after drop should fail
            server.sendall("create drop_in_prog\n")
            resp = fh.readline()
            if resp == "Delete in progress\n":
                return
            elif resp == "Done\n":
                server.sendall("drop drop_in_prog\n")
                fh.readline()

        assert False, "Failed to do a concurrent create"

    def test_create_long_prefix(self, servers):
        "Tests create with long prefix"
        server, _ = servers
        fh = server.makefile()
        server.sendall("create test:create:filter:with:long:prefix:1\n")
        server.sendall("create test:create:filter:with:long:prefix:2\n")
        server.sendall("create test:create:filter:with:long:common:1\n")
        assert fh.readline() == "Done\n"
        assert fh.readline() == "Done\n"
        assert fh.readline() == "Done\n"
        time.sleep(2) # Wait for vacuum, ensures filters are in ascending order
        server.sendall("list test:create:filter\n")
        assert fh.readline() == "START\n"
        assert "test:create:filter:with:long:common:1" in fh.readline()
        assert "test:create:filter:with:long:prefix:1" in fh.readline()
        assert "test:create:filter:with:long:prefix:2" in fh.readline()
        assert fh.readline() == "END\n"

if __name__ == "__main__":
    sys.exit(pytest.main(args="-k TestInteg."))

