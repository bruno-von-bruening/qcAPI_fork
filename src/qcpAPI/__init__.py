try:
    from ._version import __version__
except ImportError as ex:
    __version__ = "unknown"
    print(f"Warning: could not import version information: {str(ex)}")