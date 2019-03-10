import subprocess
import sys
sys.path.insert(0, './answers')
from answer import count

#Assert values from 2016 file only.
def test_count():
    a = count("./data/frenepublicinjection2016.csv")
    assert(a == 27244)
