import sys, os
import argparse


def wrap(main, argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    main(argv)
from functools import partial