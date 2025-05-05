from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '1.0.0'
DESCRIPTION = 'Fastes historical candle data Loader'
LONG_DESCRIPTION = 'A package that allows to fetch the historical data in fastest way with its own cache system'

setup(
    name="Candlestream",
    version=VERSION,
    author="Sachin Sachan",
    author_email="<sachinsachan722@gmail.com>",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(where = "CandleStream"),
    url="https://github.com/sac-1999/CandleStream",
    license="MIT",
    classifiers=[
        "license :: OSI Approved :: MIT License",
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)